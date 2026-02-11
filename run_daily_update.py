"""
일일 업데이트 스크립트 (독립 실행).

사용법:
    python run_daily_update.py              # 일일 업데이트 (오늘 주가 + MA + RS)
    python run_daily_update.py --backfill   # MA + RS 전체 백필 (초기 세팅용)

실행 순서:
    1. DB 마이그레이션 (컬럼 없으면 자동 추가)
    2. 오늘 주가 수집 (FMP, 증분)
    3. industry 정보 수집 (없는 종목만)
    4. MA 계산 (전종목)
    5. RS 계산 (최근일 or 전체 백필)
"""

import sys
import time
import requests
import pymysql
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from db.connection import get_connection
from config.settings import Settings


def migrate(conn):
    """필요한 컬럼이 없으면 추가 (테이블별 단일 ALTER TABLE로 합침)"""
    cursor = conn.cursor()

    # 기존 컬럼 확인
    cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'bs_daily_prices'")
    existing_dp = {r[0] for r in cursor.fetchall()}
    cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'bs_stocks'")
    existing_st = {r[0] for r in cursor.fetchall()}

    # bs_daily_prices: 없는 컬럼만 한번에 추가
    dp_adds = []
    for col, col_def in [
        ("ma50", "DECIMAL(12,4) DEFAULT NULL"),
        ("ma150", "DECIMAL(12,4) DEFAULT NULL"),
        ("ma200", "DECIMAL(12,4) DEFAULT NULL"),
        ("rs_1m", "TINYINT UNSIGNED DEFAULT NULL"),
        ("rs_3m", "TINYINT UNSIGNED DEFAULT NULL"),
        ("rs_6m", "TINYINT UNSIGNED DEFAULT NULL"),
    ]:
        if col not in existing_dp:
            dp_adds.append(f"ADD COLUMN {col} {col_def}")

    if dp_adds:
        sql = "ALTER TABLE bs_daily_prices " + ", ".join(dp_adds)
        print(f"  [마이그레이션] bs_daily_prices에 {len(dp_adds)}개 컬럼 추가 중... (대용량 테이블이라 수분 소요)")
        cursor.execute(sql)
        conn.commit()
        print(f"  [마이그레이션] bs_daily_prices 완료")
    else:
        print("  [마이그레이션] bs_daily_prices 컬럼 이미 존재")

    # bs_stocks: industry
    if "industry" not in existing_st:
        print("  [마이그레이션] bs_stocks.industry 추가 중...")
        cursor.execute("ALTER TABLE bs_stocks ADD COLUMN industry VARCHAR(100) DEFAULT NULL")
        conn.commit()
        print("  [마이그레이션] bs_stocks.industry 완료")
    else:
        print("  [마이그레이션] bs_stocks.industry 이미 존재")


def collect_today_prices(conn, api_key):
    """FMP로 오늘 주가 증분 수집"""
    from services.price_collector import collect_prices_fmp

    # 시작일: DB 최신일 기준
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(trade_date) FROM bs_daily_prices")
    latest = cursor.fetchone()[0]
    if latest:
        start_date = str(latest)
    else:
        start_date = "2016-01-01"

    print(f"[주가 수집] {start_date} 이후 데이터 수집...")
    candles = collect_prices_fmp(conn, api_key, start_date, include_delisted=False)
    print(f"[주가 수집] 완료: {candles:,}개 캔들 추가")
    return candles


def _fetch_profile(api_key, ticker):
    """FMP profile에서 industry 가져오기"""
    try:
        resp = requests.get(
            "https://financialmodelingprep.com/stable/profile",
            params={"symbol": ticker, "apikey": api_key},
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            if data:
                item = data[0] if isinstance(data, list) else data
                return item.get("industry")
    except Exception:
        pass
    return None


def collect_industry(conn, api_key):
    """industry 정보가 없는 종목만 FMP에서 수집"""
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(
        "SELECT id, ticker FROM bs_stocks WHERE industry IS NULL AND is_active = 1"
    )
    stocks = cursor.fetchall()
    if not stocks:
        print("[산업 수집] 모든 활성 종목에 industry 정보가 있습니다.")
        return

    total = len(stocks)
    print(f"[산업 수집] {total}개 종목 industry 수집 시작...")
    update_count = 0
    BATCH = 50
    WORKERS = 10

    for batch_start in range(0, total, BATCH):
        batch = stocks[batch_start:batch_start + BATCH]

        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {
                executor.submit(_fetch_profile, api_key, s["ticker"]): s
                for s in batch
            }
            for future in as_completed(futures):
                s = futures[future]
                try:
                    industry = future.result()
                    if industry:
                        cursor.execute(
                            "UPDATE bs_stocks SET industry = %s WHERE id = %s",
                            (industry, s["id"]),
                        )
                        update_count += 1
                except Exception:
                    pass

        conn.commit()
        print(
            f"  진행: {min(batch_start + BATCH, total)}/{total} "
            f"(업데이트 {update_count}개)",
            flush=True,
        )
        time.sleep(0.3)

    print(f"[산업 수집] 완료: {update_count}개 종목 industry 업데이트")


def _needs_price_update(conn):
    """최신 주가가 오늘/어제(거래일)인지 확인 → 이미 최신이면 False"""
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(trade_date) FROM bs_daily_prices")
    latest = cursor.fetchone()[0]
    if not latest:
        return True
    from datetime import date
    days_old = (date.today() - latest).days
    return days_old > 1  # 주말 고려, 1일 이하면 최신

def _needs_ma_update(conn):
    """MA가 최신 날짜에 채워져 있는지 확인"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM bs_daily_prices
        WHERE trade_date = (SELECT MAX(trade_date) FROM bs_daily_prices)
          AND ma50 IS NOT NULL
    """)
    return cursor.fetchone()[0] == 0


def main():
    backfill = "--backfill" in sys.argv

    settings = Settings()
    if not settings.FMP_API_KEY:
        print("[에러] .env에 FMP_API_KEY가 설정되지 않았습니다.")
        return

    conn = get_connection()
    start = datetime.now()

    try:
        # 1. 마이그레이션
        print("=" * 50)
        print("[1/5] DB 마이그레이션")
        print("=" * 50)
        migrate(conn)

        # 2. 주가 수집 (이미 최신이면 스킵)
        print()
        print("=" * 50)
        print("[2/5] 주가 수집 (FMP)")
        print("=" * 50)
        if _needs_price_update(conn):
            collect_today_prices(conn, settings.FMP_API_KEY)
        else:
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(trade_date) FROM bs_daily_prices")
            print(f"  최신 데이터: {cursor.fetchone()[0]} → 스킵")

        # 3. industry 수집
        print()
        print("=" * 50)
        print("[3/5] Industry 정보 수집")
        print("=" * 50)
        collect_industry(conn, settings.FMP_API_KEY)

        # 4. MA 계산 (이미 최신이면 스킵)
        print()
        print("=" * 50)
        print("[4/5] 이동평균선 계산 (MA50/150/200)")
        print("=" * 50)
        if _needs_ma_update(conn) or backfill:
            from services.ma_calculator import calculate_moving_averages
            calculate_moving_averages(conn)
        else:
            print("  MA 이미 최신 → 스킵")

        # 5. RS 계산
        print()
        print("=" * 50)
        print("[5/5] 상대강도(RS) 계산")
        print("=" * 50)
        from services.rs_calculator import calculate_rs
        calculate_rs(conn, backfill=backfill)

    finally:
        conn.close()

    elapsed = datetime.now() - start
    print()
    print("=" * 50)
    print(f"전체 완료! (소요시간: {elapsed})")
    print("=" * 50)


if __name__ == "__main__":
    main()
