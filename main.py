"""
Breakout Spotter CLI

Usage:
    python main.py init              # asset_us DB에 bs_* 테이블 생성
    python main.py collect-symbols   # 미국 전 종목 목록 수집
    python main.py collect-prices    # 주가 수집 (FMP → yfinance → KIS 폴백)
    python main.py collect-prices --reset  # 기존 데이터 삭제 후 재수집
    python main.py collect-financials      # 재무 데이터 수집 (FMP)
    python main.py collect-financials --include-delisted  # 상장폐지 종목 포함
    python main.py collect-marketcap       # 역사적 시가총액 수집 (FMP)
    python main.py collect-marketcap --include-delisted  # 상장폐지 종목 포함
    python main.py calculate-ma           # 이동평균선(MA50/150/200) 계산
    python main.py calculate-rs           # 상대강도(RS) 계산 (최근일)
    python main.py calculate-rs --backfill # RS 전체 백필
    python main.py collect-industry       # FMP에서 industry 수집
    python main.py sync-themes       # theme_analyzer에서 테마 동기화
    python main.py scan              # 돌파 패턴 스캔
    python main.py backtest          # 백테스트
    python main.py backtest --include-delisted  # 생존편향 제거 백테스트
    python main.py full              # 위 전체 순차 실행
    python main.py status            # DB 현황 조회
"""

import sys
from pathlib import Path
from db.connection import get_connection


def init_db():
    """bs_* 테이블 생성 + 마이그레이션"""
    print("[DB 초기화] 시작...")
    schema_path = Path(__file__).parent / "db" / "schema.sql"
    sql = schema_path.read_text(encoding="utf-8")

    conn = get_connection()
    cursor = conn.cursor()

    for statement in sql.split(";"):
        statement = statement.strip()
        if statement:
            cursor.execute(statement)

    # 마이그레이션
    migrations = [
        ("bs_daily_prices", "ma50", "DECIMAL(12,4) DEFAULT NULL"),
        ("bs_daily_prices", "ma150", "DECIMAL(12,4) DEFAULT NULL"),
        ("bs_daily_prices", "ma200", "DECIMAL(12,4) DEFAULT NULL"),
        ("bs_daily_prices", "rs_1m", "TINYINT UNSIGNED DEFAULT NULL"),
        ("bs_daily_prices", "rs_3m", "TINYINT UNSIGNED DEFAULT NULL"),
        ("bs_daily_prices", "rs_6m", "TINYINT UNSIGNED DEFAULT NULL"),
        ("bs_stocks", "industry", "VARCHAR(100) DEFAULT NULL"),
    ]
    for table, col, col_def in migrations:
        try:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_def}")
        except Exception:
            pass  # 이미 존재

    conn.commit()
    conn.close()
    print("[DB 초기화] 완료: bs_* 테이블 생성됨")


def run_collect_symbols():
    from services.symbol_collector import collect_symbols
    conn = get_connection()
    try:
        collect_symbols(conn)
    finally:
        conn.close()


def run_collect_prices(reset=False):
    from services.price_collector import collect_prices
    from services.kis_service import KISClient
    conn = get_connection()
    if reset:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM bs_daily_prices")
        count = cursor.fetchone()[0]
        print(f"[주가 초기화] bs_daily_prices 테이블 비우기 ({count:,}행 삭제)...")
        cursor.execute("TRUNCATE TABLE bs_daily_prices")
        conn.commit()
        print("[주가 초기화] 완료")
    kis = KISClient()
    try:
        collect_prices(conn, kis_client=kis)
    finally:
        conn.close()


def run_collect_financials(include_delisted=False):
    from services.fundamental_collector import collect_financials
    conn = get_connection()
    try:
        collect_financials(conn, include_delisted=include_delisted)
    finally:
        conn.close()


def run_collect_marketcap(include_delisted=False):
    from services.fundamental_collector import collect_market_caps
    conn = get_connection()
    try:
        collect_market_caps(conn, include_delisted=include_delisted)
    finally:
        conn.close()


def run_sync_themes():
    from services.theme_loader import sync_themes
    conn = get_connection()
    try:
        sync_themes(conn)
    finally:
        conn.close()


def run_scan():
    from services.breakout_scanner import scan_breakouts
    conn = get_connection()
    try:
        scan_breakouts(conn)
    finally:
        conn.close()


def run_calculate_ma():
    from services.ma_calculator import calculate_moving_averages
    conn = get_connection()
    try:
        calculate_moving_averages(conn)
    finally:
        conn.close()


def run_calculate_rs(backfill=False):
    from services.rs_calculator import calculate_rs
    conn = get_connection()
    try:
        calculate_rs(conn, backfill=backfill)
    finally:
        conn.close()


def run_collect_industry():
    from run_daily_update import collect_industry
    from config.settings import Settings
    settings = Settings()
    conn = get_connection()
    try:
        collect_industry(conn, settings.FMP_API_KEY)
    finally:
        conn.close()


def run_backtest_cmd(include_delisted=False):
    from services.backtester import run_backtest
    conn = get_connection()
    try:
        run_backtest(conn, include_delisted=include_delisted)
    finally:
        conn.close()


def show_status():
    """DB 현황 조회"""
    conn = get_connection()
    cursor = conn.cursor()

    tables = [
        ("bs_stocks", "종목"),
        ("bs_daily_prices", "일봉"),
        ("bs_financials", "재무"),
        ("bs_earnings", "어닝"),
        ("bs_themes", "테마"),
        ("bs_stock_themes", "테마-종목 매핑"),
        ("bs_market_cap", "시가총액"),
        ("bs_breakout_signals", "돌파 신호"),
    ]

    print("\n[DB 현황]")
    print(f"{'테이블':<25} {'설명':<15} {'행 수':>10}")
    print("-" * 55)

    for table, desc in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table:<25} {desc:<15} {count:>10,}")
        except Exception:
            print(f"{table:<25} {desc:<15} {'(없음)':>10}")

    # 종목 활성/폐지 현황
    try:
        cursor.execute("SELECT COUNT(*) FROM bs_stocks WHERE is_active = 1")
        active = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM bs_stocks WHERE is_active = 0")
        delisted = cursor.fetchone()[0]
        print(f"\n  종목: 활성 {active:,}개, 상장폐지 {delisted:,}개")
    except Exception:
        pass

    # 주가 데이터 커버리지
    try:
        cursor.execute(
            """SELECT COUNT(DISTINCT stock_id) as stocks,
                      MIN(trade_date) as min_date,
                      MAX(trade_date) as max_date
               FROM bs_daily_prices"""
        )
        row = cursor.fetchone()
        if row and row[0]:
            print(f"  주가 데이터: {row[0]:,}종목, {row[1]} ~ {row[2]}")
    except Exception:
        pass

    # 최근 신호
    try:
        cursor.execute(
            """SELECT s.ticker, b.signal_date, b.signal_score, b.rise_from_low_pct
               FROM bs_breakout_signals b
               JOIN bs_stocks s ON b.stock_id = s.id
               ORDER BY b.signal_date DESC, b.signal_score DESC
               LIMIT 5"""
        )
        rows = cursor.fetchall()
        if rows:
            print(f"\n  최근 돌파 신호:")
            for r in rows:
                print(f"    {r[0]:>8} | {r[1]} | 점수 {r[2]:>5.0f} | 상승 {r[3]:>6.1f}%")
    except Exception:
        pass

    conn.close()


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return

    command = sys.argv[1]
    flags = sys.argv[2:]

    commands = {
        "init": init_db,
        "collect-symbols": run_collect_symbols,
        "collect-prices": lambda: run_collect_prices(reset="--reset" in flags),
        "collect-financials": lambda: run_collect_financials(include_delisted="--include-delisted" in flags),
        "collect-marketcap": lambda: run_collect_marketcap(include_delisted="--include-delisted" in flags),
        "calculate-ma": run_calculate_ma,
        "calculate-rs": lambda: run_calculate_rs(backfill="--backfill" in flags),
        "collect-industry": run_collect_industry,
        "sync-themes": run_sync_themes,
        "scan": run_scan,
        "backtest": lambda: run_backtest_cmd(include_delisted="--include-delisted" in flags),
        "status": show_status,
        "full": lambda: (
            run_collect_symbols(),
            run_collect_financials(),
            run_collect_prices(),
            run_calculate_ma(),
            run_calculate_rs(),
            run_sync_themes(),
            run_scan(),
        ),
    }

    if command not in commands:
        print(f"알 수 없는 명령: {command}")
        print(__doc__)
        return

    try:
        commands[command]()
    except KeyboardInterrupt:
        print("\n중단됨")
    except Exception as e:
        print(f"\n[에러] {e}")
        raise


if __name__ == "__main__":
    main()
