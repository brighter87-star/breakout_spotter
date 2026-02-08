"""
미국 상장 전 종목 목록 수집.
NASDAQ API에서 NYSE, NASDAQ, AMEX 종목 리스트를 가져와 bs_stocks에 저장.
"""

import requests
import pymysql

EXCHANGE_MAP = {
    "NASDAQ": "NAS",
    "NYSE": "NYS",
    "AMEX": "AMS",
    "NAS": "NAS",
    "NYS": "NYS",
    "AMS": "AMS",
}

# NASDAQ screener API (공개)
NASDAQ_API_URL = "https://api.nasdaq.com/api/screener/stocks"
NASDAQ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def fetch_stock_list():
    """NASDAQ screener API에서 NYSE+NASDAQ+AMEX 전 종목 가져오기"""
    all_stocks = []

    for exchange in ["NASDAQ", "NYSE", "AMEX"]:
        print(f"  [{exchange}] 종목 목록 다운로드 중...")
        params = {
            "tableonly": "true",
            "limit": 10000,
            "offset": 0,
            "exchange": exchange.lower(),
        }

        try:
            resp = requests.get(NASDAQ_API_URL, params=params, headers=NASDAQ_HEADERS, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            rows = data.get("data", {}).get("table", {}).get("rows", [])
            if not rows:
                rows = data.get("data", {}).get("rows", [])

            for row in rows:
                ticker = (row.get("symbol") or row.get("Symbol") or "").strip()
                name = (row.get("name") or row.get("Name") or "").strip()

                if not ticker or len(ticker) > 10:
                    continue
                # 우선주/워런트 등 제외
                if any(c in ticker for c in ["^", "/"]):
                    continue

                # 시가총액 파싱
                mcap_str = (row.get("marketCap") or "").replace(",", "").strip()
                market_cap = int(mcap_str) if mcap_str.isdigit() else None

                all_stocks.append({
                    "ticker": ticker,
                    "name": name[:200],
                    "exchange": exchange,
                    "exchange_code": EXCHANGE_MAP[exchange],
                    "market_cap": market_cap,
                })

            print(f"  [{exchange}] {len(rows)}개 종목 확인")

        except Exception as e:
            print(f"  [{exchange}] 실패: {e}")
            # 폴백: yfinance 등 다른 소스 시도 가능
            continue

    return all_stocks


def save_stocks_to_db(conn, stocks):
    """bs_stocks 테이블에 종목 목록 저장 (upsert)"""
    cursor = conn.cursor()
    inserted = 0
    updated = 0

    for s in stocks:
        try:
            cursor.execute(
                """INSERT INTO bs_stocks (ticker, name, exchange, exchange_code, market_cap)
                   VALUES (%s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                       name = VALUES(name),
                       exchange = VALUES(exchange),
                       exchange_code = VALUES(exchange_code),
                       market_cap = COALESCE(VALUES(market_cap), market_cap),
                       is_active = 1""",
                (s["ticker"], s["name"], s["exchange"], s["exchange_code"], s.get("market_cap")),
            )
            if cursor.rowcount == 1:
                inserted += 1
            elif cursor.rowcount == 2:
                updated += 1
        except pymysql.Error as e:
            print(f"  [WARN] {s['ticker']}: {e}")

    conn.commit()
    return {"inserted": inserted, "updated": updated, "total": len(stocks)}


def ensure_spy(conn):
    """SPY(벤치마크)가 bs_stocks에 존재하는지 확인"""
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM bs_stocks WHERE ticker = 'SPY'")
    if not cursor.fetchone():
        cursor.execute(
            """INSERT INTO bs_stocks (ticker, name, exchange, exchange_code)
               VALUES ('SPY', 'SPDR S&P 500 ETF Trust', 'NYSE', 'NYS')""",
        )
        conn.commit()
        print("  SPY(벤치마크) 추가 완료")


def collect_symbols(conn):
    """종목 수집 메인 함수"""
    print("[종목 수집] 시작...")
    stocks = fetch_stock_list()

    if not stocks:
        print("[종목 수집] 종목 리스트를 가져오지 못했습니다.")
        return

    result = save_stocks_to_db(conn, stocks)
    ensure_spy(conn)
    print(f"[종목 수집] 완료: 신규 {result['inserted']}개, 업데이트 {result['updated']}개, 총 {result['total']}개")
