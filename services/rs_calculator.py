"""
상대강도(RS) 백분위 계산.
1개월(21거래일), 3개월(63거래일), 6개월(126거래일) 수익률을
전종목 대비 순위로 환산하여 0~99 정수로 저장.
"""

import pymysql
import pandas as pd
import numpy as np

RS_PERIODS = {"rs_1m": 21, "rs_3m": 63, "rs_6m": 126}


def calculate_rs(conn, backfill=False):
    """전종목 RS 백분위 계산 → bs_daily_prices 업데이트"""
    cursor = conn.cursor()

    # 1) 전종목 종가 로드 → 피벗 테이블
    print("[RS] 종가 데이터 로딩...")
    cursor.execute(
        "SELECT stock_id, trade_date, close_price FROM bs_daily_prices ORDER BY trade_date"
    )
    rows = cursor.fetchall()
    if not rows:
        print("[RS] 데이터 없음")
        return

    df = pd.DataFrame(rows, columns=["stock_id", "trade_date", "close_price"])
    pivot = df.pivot(index="trade_date", columns="stock_id", values="close_price")
    pivot.sort_index(inplace=True)
    print(f"[RS] 피벗: {len(pivot)}일 x {len(pivot.columns)}종목")

    # 2) 수익률 → 순위 → 백분위 (벡터 연산)
    rs_frames = {}
    for col_name, period in RS_PERIODS.items():
        print(f"[RS] {col_name} (기간={period}) 계산 중...")
        returns = pivot / pivot.shift(period) - 1
        # 날짜별 순위 (axis=1: 종목 간 비교)
        ranks = returns.rank(axis=1, method="average")
        counts = returns.count(axis=1)  # 날짜별 유효 종목 수
        # 백분위: (rank-1)/(n-1)*99, 0~99 정수
        percentiles = ranks.sub(1).div(counts.sub(1), axis=0).mul(99).round()
        percentiles = percentiles.clip(0, 99)
        rs_frames[col_name] = percentiles

    # 3) 처리할 날짜 필터링
    if backfill:
        # 126거래일 이후부터 (rs_6m 계산 가능한 날짜)
        valid_start = pivot.index[126] if len(pivot) > 126 else pivot.index[-1]
        target_dates = [d for d in pivot.index if d >= valid_start]
        print(f"[RS] 백필: {len(target_dates)}일 ({target_dates[0]} ~ {target_dates[-1]})")
    else:
        target_dates = [pivot.index[-1]]
        print(f"[RS] 최근일: {target_dates[0]}")

    # 4) DB 업데이트 (배치)
    update_sql = """
        UPDATE bs_daily_prices
        SET rs_1m = %s, rs_3m = %s, rs_6m = %s
        WHERE stock_id = %s AND trade_date = %s
    """
    total_dates = len(target_dates)
    update_count = 0

    for i, trade_date in enumerate(target_dates):
        batch = []
        for stock_id in pivot.columns:
            vals = {}
            for col_name in RS_PERIODS:
                v = rs_frames[col_name].loc[trade_date, stock_id]
                vals[col_name] = int(v) if pd.notna(v) else None

            if vals.get("rs_1m") is not None:
                batch.append((
                    vals.get("rs_1m"), vals.get("rs_3m"), vals.get("rs_6m"),
                    int(stock_id), trade_date
                ))

        if batch:
            cursor.executemany(update_sql, batch)
            update_count += len(batch)

        if (i + 1) % 20 == 0 or i + 1 == total_dates:
            conn.commit()
            print(f"  진행: {i+1}/{total_dates}일 ({update_count:,}행)", flush=True)

    conn.commit()
    print(f"[RS] 완료: {update_count:,}행 업데이트")
