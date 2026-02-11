"""
상대강도(RS) 백분위 계산.
1개월, 3개월, 6개월 (캘린더 월 기준) 수익률을
전종목 대비 순위로 환산하여 0~99 정수로 저장.
"""

import pymysql
import pandas as pd
import numpy as np

RS_PERIODS = {"rs_1m": 1, "rs_3m": 3, "rs_6m": 6}  # 캘린더 월


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
    df["close_price"] = df["close_price"].astype(float)  # Decimal → float
    pivot = df.pivot(index="trade_date", columns="stock_id", values="close_price")
    pivot.sort_index(inplace=True)
    pivot.index = pd.to_datetime(pivot.index)

    print(f"[RS] 피벗: {len(pivot)}일 x {len(pivot.columns)}종목")

    # 2) 캘린더 월 기준 수익률 → 순위 → 백분위
    date_to_row = {d: i for i, d in enumerate(pivot.index)}
    rs_frames = {}

    for col_name, months in RS_PERIODS.items():
        print(f"[RS] {col_name} ({months}개월) 계산 중...")

        # 각 날짜의 N개월 전 가장 가까운 거래일 찾기
        lookback_dates = pivot.index - pd.DateOffset(months=months)
        prev_prices = np.full_like(pivot.values, np.nan)

        for i, lb_date in enumerate(lookback_dates):
            nearest = pivot.index.asof(lb_date)
            if pd.notna(nearest):
                row_idx = date_to_row.get(nearest)
                if row_idx is not None:
                    prev_prices[i] = pivot.values[row_idx]

        # 수익률 계산
        with np.errstate(divide="ignore", invalid="ignore"):
            returns_arr = pivot.values / prev_prices - 1

        returns_df = pd.DataFrame(returns_arr, index=pivot.index, columns=pivot.columns)

        # 날짜별 순위 → 백분위 (0~99)
        ranks = returns_df.rank(axis=1, method="average")
        counts = returns_df.count(axis=1)
        percentiles = ranks.sub(1).div(counts.sub(1), axis=0).mul(99).round()
        percentiles = percentiles.clip(0, 99)
        rs_frames[col_name] = percentiles

    # 3) 처리할 날짜 필터링
    if backfill:
        # 6개월 이후부터 유효
        six_months_after = pivot.index[0] + pd.DateOffset(months=6)
        target_dates = pivot.index[pivot.index >= six_months_after]
        print(f"[RS] 백필: {len(target_dates)}일 ({target_dates[0].date()} ~ {target_dates[-1].date()})")
    else:
        target_dates = pivot.index[-1:]
        print(f"[RS] 최근일: {target_dates[0].date()}")

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
        trade_date_str = trade_date.strftime("%Y-%m-%d")

        for stock_id in pivot.columns:
            vals = {}
            for col_name in RS_PERIODS:
                v = rs_frames[col_name].at[trade_date, stock_id]
                vals[col_name] = int(v) if pd.notna(v) else None

            if vals.get("rs_1m") is not None:
                batch.append((
                    vals.get("rs_1m"), vals.get("rs_3m"), vals.get("rs_6m"),
                    int(stock_id), trade_date_str,
                ))

        if batch:
            cursor.executemany(update_sql, batch)
            update_count += len(batch)

        if (i + 1) % 20 == 0 or i + 1 == total_dates:
            conn.commit()
            print(f"  진행: {i+1}/{total_dates}일 ({update_count:,}행)", flush=True)

    conn.commit()
    print(f"[RS] 완료: {update_count:,}행 업데이트")
