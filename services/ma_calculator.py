"""
이동평균선(MA50/150/200) 계산.
bs_daily_prices 테이블의 ma50, ma150, ma200 컬럼 업데이트.
"""

import pymysql

MA_PERIODS = [50, 150, 200]

_UPDATE_SQL = """
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


def calculate_moving_averages(conn, stock_ids=None):
    """전 종목 MA50/150/200 계산 → bs_daily_prices 업데이트"""
    cursor = conn.cursor()

    if stock_ids is None:
        cursor.execute("SELECT DISTINCT stock_id FROM bs_daily_prices ORDER BY stock_id")
        stock_ids = [r[0] for r in cursor.fetchall()]

    total = len(stock_ids)
    print(f"[이동평균] {total}개 종목 MA50/150/200 계산 시작...")

    for i, sid in enumerate(stock_ids):
        cursor.execute(_UPDATE_SQL, (sid,))

        if (i + 1) % 100 == 0 or i + 1 == total:
            conn.commit()
            print(f"  진행: {i+1}/{total}", flush=True)

    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM bs_daily_prices WHERE ma200 IS NOT NULL")
    filled = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM bs_daily_prices")
    total_rows = cursor.fetchone()[0]
    print(f"[이동평균] 완료: {filled:,}/{total_rows:,}행 MA200 채워짐")
