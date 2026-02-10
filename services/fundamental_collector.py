"""
재무 데이터 수집 (FMP API).
bs_financials, bs_earnings 테이블에 데이터 저장.
상장폐지 종목 수집 지원.
"""

import time
import requests
import pymysql
from concurrent.futures import ThreadPoolExecutor, as_completed

FMP_BASE = "https://financialmodelingprep.com"
MAX_RETRIES = 3


def _get_all_stocks(conn, include_delisted=False):
    """종목 목록 조회"""
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    if include_delisted:
        cursor.execute("SELECT id, ticker FROM bs_stocks ORDER BY id")
    else:
        cursor.execute("SELECT id, ticker FROM bs_stocks WHERE is_active = 1 ORDER BY id")
    return cursor.fetchall()


def _fmp_get(endpoint, api_key, params=None):
    """FMP API GET 요청"""
    params = params or {}
    params["apikey"] = api_key
    resp = requests.get(f"{FMP_BASE}{endpoint}", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


# ── 상장폐지 종목 수집 ──────────────────────────────────────

def _collect_delisted_symbols(conn, api_key):
    """FMP에서 상장폐지 종목 목록 수집 → bs_stocks에 추가"""
    print("[상장폐지 종목] 수집 시작...")

    try:
        data = _fmp_get("/stable/delisted-companies", api_key, {"page": 0, "limit": 10000})
    except Exception as e:
        print(f"  [ERR] 상장폐지 목록 조회 실패: {e}")
        return 0

    if not data:
        print("  데이터 없음")
        return 0

    cursor = conn.cursor()
    count = 0
    for item in data:
        symbol = item.get("symbol", "")
        if not symbol or "/" in symbol or "^" in symbol:
            continue

        name = item.get("companyName", "")
        exchange = item.get("exchange", "")
        delisted_date = item.get("delistedDate")

        try:
            cursor.execute(
                """INSERT INTO bs_stocks (ticker, name, exchange, is_active, delisted_date)
                   VALUES (%s, %s, %s, 0, %s)
                   ON DUPLICATE KEY UPDATE
                       is_active = 0,
                       delisted_date = COALESCE(VALUES(delisted_date), delisted_date)""",
                (symbol, name[:200] if name else None, exchange[:10] if exchange else None, delisted_date),
            )
            count += cursor.rowcount
        except pymysql.Error:
            pass

    conn.commit()
    print(f"[상장폐지 종목] 완료: {count}개 추가/업데이트 (전체 {len(data)}개)")
    return count


# ── Income Statement 수집 ──────────────────────────────────

def _insert_financials(conn, stock_id, records):
    """연간 income statement INSERT"""
    cursor = conn.cursor()
    count = 0
    for rec in records:
        period = rec.get("period", "")
        if period != "FY":
            continue

        filing_date = rec.get("fillingDate") or rec.get("filingDate")
        period_end = rec.get("date")
        if not filing_date or not period_end:
            continue

        fiscal_year = rec.get("fiscalYear") or rec.get("calendarYear")
        if not fiscal_year:
            try:
                fiscal_year = int(period_end[:4])
            except (ValueError, TypeError):
                continue

        revenue = rec.get("revenue")
        net_income = rec.get("netIncome")
        eps = rec.get("epsDiluted") or rec.get("epsdiluted")

        try:
            cursor.execute(
                """INSERT IGNORE INTO bs_financials
                   (stock_id, fiscal_year, period_end, filing_date, revenue, net_income, eps_diluted)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (stock_id, int(fiscal_year), period_end, filing_date, revenue, net_income, eps),
            )
            count += cursor.rowcount
        except pymysql.Error:
            pass
    return count


def _fetch_one(endpoint, api_key, params, stock):
    """단일 종목 HTTP 요청 (스레드용). 429시 exponential backoff 재시도."""
    for attempt in range(MAX_RETRIES + 1):
        try:
            data = _fmp_get(endpoint, api_key, params)
            if data and isinstance(data, list):
                return (stock, data, None)
            return (stock, None, None)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                return (stock, None, None)
            if e.response is not None and e.response.status_code == 429 and attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
                continue
            return (stock, None, e)
        except Exception as e:
            return (stock, None, e)
    return (stock, None, None)


def _collect_income_statements(conn, api_key, stocks):
    """전 종목 연간 income statement 수집 (멀티스레드)"""
    print("[재무 수집] Income Statement 시작...")
    total = len(stocks)
    total_inserted = 0
    error_count = 0
    BATCH = 50
    WORKERS = 15

    for batch_start in range(0, total, BATCH):
        batch = stocks[batch_start:batch_start + BATCH]
        print(f"  진행: {batch_start}/{total} (누적 {total_inserted}개, 에러 {error_count}개)", flush=True)

        # 병렬 HTTP 요청
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = [
                executor.submit(
                    _fetch_one, "/stable/income-statement", api_key,
                    {"symbol": s["ticker"], "period": "annual", "limit": 30}, s
                )
                for s in batch
            ]
            for future in as_completed(futures):
                stock, data, err = future.result()
                if err:
                    error_count += 1
                    if error_count <= 10:
                        print(f"  [ERR] {stock['ticker']}: {err}")
                elif data:
                    inserted = _insert_financials(conn, stock["id"], data)
                    total_inserted += inserted

        conn.commit()

    if error_count > 10:
        print(f"  [WARN] 총 {error_count}개 에러")
    print(f"[재무 수집] Income Statement 완료: +{total_inserted}개")
    return total_inserted


# ── Earnings 수집 ──────────────────────────────────────────

def _insert_earnings(conn, stock_id, records):
    """earnings 데이터 INSERT"""
    cursor = conn.cursor()
    count = 0
    for rec in records:
        earnings_date = rec.get("date")
        if not earnings_date:
            continue

        try:
            cursor.execute(
                """INSERT IGNORE INTO bs_earnings
                   (stock_id, earnings_date, eps_estimated, eps_actual,
                    revenue_estimated, revenue_actual, time_of_day)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (
                    stock_id, earnings_date,
                    rec.get("epsEstimated"), rec.get("epsActual") or rec.get("eps"),
                    rec.get("revenueEstimated"), rec.get("revenueActual") or rec.get("revenue"),
                    rec.get("time"),
                ),
            )
            count += cursor.rowcount
        except pymysql.Error:
            pass
    return count


def _collect_earnings(conn, api_key, stocks):
    """전 종목 earnings 데이터 수집 (멀티스레드)"""
    print("[재무 수집] Earnings 시작...")
    total = len(stocks)
    total_inserted = 0
    error_count = 0
    BATCH = 50
    WORKERS = 15

    for batch_start in range(0, total, BATCH):
        batch = stocks[batch_start:batch_start + BATCH]
        print(f"  진행: {batch_start}/{total} (누적 {total_inserted}개, 에러 {error_count}개)", flush=True)

        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = [
                executor.submit(
                    _fetch_one, "/stable/earnings", api_key,
                    {"symbol": s["ticker"], "limit": 100}, s
                )
                for s in batch
            ]
            for future in as_completed(futures):
                stock, data, err = future.result()
                if err:
                    error_count += 1
                    if error_count <= 10:
                        print(f"  [ERR] {stock['ticker']}: {err}")
                elif data:
                    inserted = _insert_earnings(conn, stock["id"], data)
                    total_inserted += inserted

        conn.commit()

    if error_count > 10:
        print(f"  [WARN] 총 {error_count}개 에러")
    print(f"[재무 수집] Earnings 완료: +{total_inserted}개")
    return total_inserted


# ── 메인 함수 ──────────────────────────────────────────────

def collect_financials(conn, include_delisted=False):
    """재무 데이터 수집 메인 함수"""
    from config.settings import Settings
    settings = Settings()
    api_key = settings.FMP_API_KEY

    if not api_key:
        print("[재무 수집] FMP_API_KEY가 설정되지 않았습니다. .env 파일을 확인하세요.")
        return

    # 1. 상장폐지 종목 추가 (옵션)
    if include_delisted:
        _collect_delisted_symbols(conn, api_key)

    # 2. 전체 종목 목록 (활성 + 폐지 모두)
    stocks = _get_all_stocks(conn, include_delisted=True)
    print(f"[재무 수집] 대상: {len(stocks)}개 종목")

    # 3. Income Statement 수집
    _collect_income_statements(conn, api_key, stocks)

    # 4. Earnings 수집
    _collect_earnings(conn, api_key, stocks)

    # 5. 통계
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(DISTINCT stock_id) FROM bs_financials")
    fin_stocks = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM bs_financials")
    fin_rows = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT stock_id) FROM bs_earnings")
    earn_stocks = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM bs_earnings")
    earn_rows = cursor.fetchone()[0]
    print(f"[재무 수집] DB 현황: 재무 {fin_stocks}종목/{fin_rows}행, 어닝 {earn_stocks}종목/{earn_rows}행")
