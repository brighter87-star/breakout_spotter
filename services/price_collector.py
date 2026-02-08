"""
주가 데이터 수집 (yfinance 1차, KIS API 2차).
bs_daily_prices 테이블에 12개월+ OHLCV 데이터 저장.
"""

import pymysql
from datetime import datetime, timedelta


def _get_all_stocks(conn):
    """활성 종목 목록 조회"""
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(
        "SELECT id, ticker, exchange_code FROM bs_stocks WHERE is_active = 1 ORDER BY id"
    )
    return cursor.fetchall()


def _get_latest_date(conn, stock_id):
    """종목의 가장 최근 데이터 날짜 조회"""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT MAX(trade_date) FROM bs_daily_prices WHERE stock_id = %s",
        (stock_id,),
    )
    row = cursor.fetchone()
    return row[0] if row and row[0] else None


def _insert_prices(conn, stock_id, prices, after_date=None):
    """가격 데이터 INSERT (중복 시 무시)"""
    cursor = conn.cursor()
    count = 0
    for p in prices:
        trade_date = p["date"]
        # YYYYMMDD → DATE 변환
        if len(trade_date) == 8 and "-" not in trade_date:
            trade_date = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"

        if after_date and trade_date <= str(after_date):
            continue

        try:
            cursor.execute(
                """INSERT IGNORE INTO bs_daily_prices
                   (stock_id, trade_date, open_price, high_price, low_price, close_price, volume)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (stock_id, trade_date, p["open"], p["high"], p["low"], p["close"], p["volume"]),
            )
            count += cursor.rowcount
        except pymysql.Error:
            pass
    return count


def collect_prices_yfinance(conn, start_date=None, batch_size=200, backfill=False):
    """yfinance로 전 종목 주가 수집 (초기 대량 수집용)"""
    try:
        import yfinance as yf
    except ImportError:
        print("  yfinance가 설치되어 있지 않습니다. pip install yfinance")
        return 0

    stocks = _get_all_stocks(conn)
    if not stocks:
        print("  bs_stocks에 종목이 없습니다. collect-symbols를 먼저 실행하세요.")
        return 0

    total_candles = 0
    total = len(stocks)

    # 배치 단위로 다운로드
    for batch_start in range(0, total, batch_size):
        batch = stocks[batch_start:batch_start + batch_size]
        tickers = [s["ticker"] for s in batch]
        batch_num = batch_start // batch_size + 1
        total_batches = (total + batch_size - 1) // batch_size

        print(f"  배치 {batch_num}/{total_batches} ({len(tickers)}종목) 다운로드 중...")

        try:
            dl_kwargs = dict(
                tickers=tickers,
                group_by="ticker",
                auto_adjust=True,
                threads=True,
                progress=False,
            )
            if start_date:
                dl_kwargs["start"] = start_date
            else:
                dl_kwargs["period"] = "1y"
            data = yf.download(**dl_kwargs)
        except Exception as e:
            print(f"  배치 {batch_num} 다운로드 실패: {e}")
            continue

        for s in batch:
            ticker = s["ticker"]
            stock_id = s["id"]
            latest = None if backfill else _get_latest_date(conn, stock_id)

            try:
                if len(tickers) == 1:
                    df = data
                else:
                    if ticker not in data.columns.get_level_values(0):
                        continue
                    df = data[ticker]

                df = df.dropna(subset=["Close"])
                if df.empty:
                    continue

                prices = []
                for idx, row in df.iterrows():
                    date_str = idx.strftime("%Y-%m-%d")
                    prices.append({
                        "date": date_str,
                        "open": round(float(row["Open"]), 4) if row["Open"] == row["Open"] else 0,
                        "high": round(float(row["High"]), 4) if row["High"] == row["High"] else 0,
                        "low": round(float(row["Low"]), 4) if row["Low"] == row["Low"] else 0,
                        "close": round(float(row["Close"]), 4),
                        "volume": int(row["Volume"]) if row["Volume"] == row["Volume"] else 0,
                    })

                inserted = _insert_prices(conn, stock_id, prices, after_date=latest)
                total_candles += inserted

            except Exception:
                continue

        conn.commit()
        print(f"  배치 {batch_num} 완료 (누적 {total_candles}개 캔들)")

    return total_candles


def collect_prices_kis(conn, kis_client, target_days=260):
    """KIS API로 전 종목 주가 수집 (폴백/일일 업데이트용)"""
    stocks = _get_all_stocks(conn)
    if not stocks:
        print("  bs_stocks에 종목이 없습니다.")
        return 0

    total_candles = 0
    errors = []
    total = len(stocks)

    for i, s in enumerate(stocks):
        ticker = s["ticker"]
        exchange_code = s["exchange_code"] or "NAS"
        stock_id = s["id"]
        latest = _get_latest_date(conn, stock_id)

        if i % 100 == 0:
            print(f"  진행: {i}/{total} (누적 {total_candles}개 캔들, 에러 {len(errors)}개)")

        try:
            # 데이터가 있으면 최근 데이터만, 없으면 전체 수집
            if latest:
                days_missing = (datetime.now().date() - latest).days
                if days_missing <= 1:
                    continue
                prices = kis_client.get_daily_prices(ticker, exchange_code)
            else:
                prices = kis_client.get_daily_prices_paginated(ticker, exchange_code, target_days)

            if prices:
                inserted = _insert_prices(conn, stock_id, prices, after_date=latest)
                total_candles += inserted

            # 50개마다 커밋
            if i % 50 == 0:
                conn.commit()

        except Exception as e:
            errors.append(f"{ticker}: {e}")

    conn.commit()

    if errors and len(errors) <= 10:
        for err in errors:
            print(f"  [ERR] {err}")
    elif errors:
        print(f"  [ERR] 총 {len(errors)}개 에러 발생")

    return total_candles


def collect_prices(conn, kis_client=None):
    """주가 수집 메인 함수: yfinance 시도 → KIS API 폴백"""
    from config.settings import Settings
    settings = Settings()

    print("[주가 수집] 시작...")

    # 시작일 계산
    start_date = (datetime.now() - timedelta(days=settings.LOOKBACK_MONTHS * 30)).strftime("%Y-%m-%d")
    print(f"[주가 수집] 기간: {start_date} ~ 현재 ({settings.LOOKBACK_MONTHS}개월)")

    # 1차: yfinance
    print("[주가 수집] yfinance로 수집 시도...")
    backfill = settings.LOOKBACK_MONTHS > 12
    batch_size = 50 if backfill else 200
    candles = collect_prices_yfinance(conn, start_date=start_date, backfill=backfill, batch_size=batch_size)

    if candles > 0:
        print(f"[주가 수집] yfinance 완료: {candles}개 캔들 저장")
    else:
        # 2차: KIS API 폴백
        if kis_client:
            print("[주가 수집] yfinance 실패, KIS API로 폴백...")
            candles = collect_prices_kis(conn, kis_client, target_days=260)
            print(f"[주가 수집] KIS API 완료: {candles}개 캔들 저장")
        else:
            print("[주가 수집] 데이터를 수집하지 못했습니다.")

    # 통계
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(DISTINCT stock_id) FROM bs_daily_prices")
    stock_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM bs_daily_prices")
    total_rows = cursor.fetchone()[0]
    print(f"[주가 수집] DB 현황: {stock_count}개 종목, {total_rows}개 행")
