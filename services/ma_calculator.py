"""
이동평균선(MA50/150/200) 계산.
bs_daily_prices 테이블의 ma50, ma150, ma200 컬럼 업데이트.
"""

import pymysql

MA_PERIODS = [50, 150, 200]

# 전체 백필용 (종목별 전 기간)
_UPDATE_ALL_SQL = """
UPDATE bs_daily_prices p
JOIN (
    SELECT id,
        CASE WHEN ROW_NUMBER() OVER w >= 50
             THEN AVG(close_price) OVER (ORDER BY trade_date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW)
        END as ma50,
        CASE WHEN ROW_NUMBER() OVER w >= 150
             THEN AVG(close_price) OVER (ORDER BY trade_date ROWS BETWEEN 149 PRECEDING AND CURRENT ROW)
        END as ma150,
        CASE WHEN ROW_NUMBER() OVER w >= 200
             THEN AVG(close_price) OVER (ORDER BY trade_date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW)
        END as ma200
    FROM bs_daily_prices
    WHERE stock_id = %s
    WINDOW w AS (ORDER BY trade_date)
) calc ON p.id = calc.id
SET p.ma50 = calc.ma50, p.ma150 = calc.ma150, p.ma200 = calc.ma200
"""

# 일일 업데이트용 (최신일만, 최근 200일 종가로 계산)
_UPDATE_LATEST_SQL = """
UPDATE bs_daily_prices dp
SET
    ma50 = (
        SELECT AVG(sub.close_price) FROM (
            SELECT close_price FROM bs_daily_prices
            WHERE stock_id = dp.stock_id AND trade_date <= dp.trade_date
            ORDER BY trade_date DESC LIMIT 50
        ) sub
        HAVING COUNT(*) = 50
    ),
    ma150 = (
        SELECT AVG(sub.close_price) FROM (
            SELECT close_price FROM bs_daily_prices
            WHERE stock_id = dp.stock_id AND trade_date <= dp.trade_date
            ORDER BY trade_date DESC LIMIT 150
        ) sub
        HAVING COUNT(*) = 150
    ),
    ma200 = (
        SELECT AVG(sub.close_price) FROM (
            SELECT close_price FROM bs_daily_prices
            WHERE stock_id = dp.stock_id AND trade_date <= dp.trade_date
            ORDER BY trade_date DESC LIMIT 200
        ) sub
        HAVING COUNT(*) = 200
    )
WHERE dp.stock_id = %s AND dp.trade_date = %s
"""


def calculate_moving_averages(conn, stock_ids=None, latest_only=False):
    """전 종목 MA50/150/200 계산 → bs_daily_prices 업데이트

    latest_only=True: 최신 거래일만 계산 (일일 업데이트용, 빠름)
    latest_only=False: 전 기간 계산 (초기 백필용, 느림)
    """
    cursor = conn.cursor()

    if stock_ids is None:
        cursor.execute("SELECT DISTINCT stock_id FROM bs_daily_prices ORDER BY stock_id")
        stock_ids = [r[0] for r in cursor.fetchall()]

    total = len(stock_ids)

    if latest_only:
        # 최신 거래일 확인
        cursor.execute("SELECT MAX(trade_date) FROM bs_daily_prices")
        latest_date = cursor.fetchone()[0]
        if not latest_date:
            print("[이동평균] 데이터 없음")
            return

        # 최신일에 데이터가 있는 종목만
        cursor.execute(
            "SELECT DISTINCT stock_id FROM bs_daily_prices WHERE trade_date = %s",
            (latest_date,),
        )
        stock_ids = [r[0] for r in cursor.fetchall()]
        total = len(stock_ids)

        print(f"[이동평균] {latest_date} 최신일 {total}개 종목 MA 계산...")

        for i, sid in enumerate(stock_ids):
            cursor.execute(_UPDATE_LATEST_SQL, (sid, latest_date))

            if (i + 1) % 500 == 0 or i + 1 == total:
                conn.commit()
                print(f"  진행: {i+1}/{total}", flush=True)
    else:
        print(f"[이동평균] {total}개 종목 MA50/150/200 전체 계산...")

        for i, sid in enumerate(stock_ids):
            cursor.execute(_UPDATE_ALL_SQL, (sid,))

            if (i + 1) % 100 == 0 or i + 1 == total:
                conn.commit()
                print(f"  진행: {i+1}/{total}", flush=True)

    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM bs_daily_prices WHERE ma200 IS NOT NULL")
    filled = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM bs_daily_prices")
    total_rows = cursor.fetchone()[0]
    print(f"[이동평균] 완료: {filled:,}/{total_rows:,}행 MA200 채워짐")
