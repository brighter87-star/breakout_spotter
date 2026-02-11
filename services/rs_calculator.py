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

        lookback_dates = pivot.index - pd.DateOffset(months=months)
        prev_prices = np.full_like(pivot.values, np.nan)

        for i, lb_date in enumerate(lookback_dates):
            nearest = pivot.index.asof(lb_date)
            if pd.notna(nearest):
                row_idx = date_to_row.get(nearest)
                if row_idx is not None:
                    prev_prices[i] = pivot.values[row_idx]

        with np.errstate(divide="ignore", invalid="ignore"):
            returns_arr = pivot.values / prev_prices - 1

        returns_df = pd.DataFrame(returns_arr, index=pivot.index, columns=pivot.columns)

        ranks = returns_df.rank(axis=1, method="average")
        counts = returns_df.count(axis=1)
        percentiles = ranks.sub(1).div(counts.sub(1), axis=0).mul(99).round()
        percentiles = percentiles.clip(0, 99)
        rs_frames[col_name] = percentiles

    # 3) 처리할 날짜 필터링 (이미 채워진 날짜 스킵)
    if backfill:
        six_months_after = pivot.index[0] + pd.DateOffset(months=6)
        all_target = pivot.index[pivot.index >= six_months_after]

        # 이미 RS가 채워진 마지막 날짜 확인 → 이어쓰기
        cursor.execute("SELECT MAX(trade_date) FROM bs_daily_prices WHERE rs_1m IS NOT NULL")
        last_filled = cursor.fetchone()[0]
        if last_filled:
            last_filled = pd.Timestamp(last_filled)
            target_dates = all_target[all_target > last_filled]
            if len(target_dates) == 0:
                print(f"[RS] 이미 모든 날짜 완료 ({last_filled.date()}까지)")
                return
            print(f"[RS] 이어쓰기: {last_filled.date()} 이후 {len(target_dates)}일")
        else:
            target_dates = all_target
            print(f"[RS] 백필: {len(target_dates)}일 ({target_dates[0].date()} ~ {target_dates[-1].date()})")
    else:
        target_dates = pivot.index[-1:]
        print(f"[RS] 최근일: {target_dates[0].date()}")

    # 4) DB 업데이트 — 임시 테이블 + JOIN UPDATE (대량 쓰기 최적화)
    total_dates = len(target_dates)

    # 날짜 청크별 처리 (메모리 절약)
    CHUNK_DAYS = 50
    update_count = 0

    for chunk_start in range(0, total_dates, CHUNK_DAYS):
        chunk_dates = target_dates[chunk_start:chunk_start + CHUNK_DAYS]

        # 임시 테이블 생성
        cursor.execute("DROP TEMPORARY TABLE IF EXISTS _tmp_rs")
        cursor.execute("""
            CREATE TEMPORARY TABLE _tmp_rs (
                stock_id INT NOT NULL,
                trade_date DATE NOT NULL,
                rs_1m TINYINT UNSIGNED,
                rs_3m TINYINT UNSIGNED,
                rs_6m TINYINT UNSIGNED,
                PRIMARY KEY (stock_id, trade_date)
            ) ENGINE=MEMORY
        """)

        # 임시 테이블에 INSERT
        insert_sql = "INSERT INTO _tmp_rs (stock_id, trade_date, rs_1m, rs_3m, rs_6m) VALUES (%s, %s, %s, %s, %s)"
        batch = []

        for trade_date in chunk_dates:
            td_str = trade_date.strftime("%Y-%m-%d")
            for stock_id in pivot.columns:
                r1 = rs_frames["rs_1m"].at[trade_date, stock_id]
                r3 = rs_frames["rs_3m"].at[trade_date, stock_id]
                r6 = rs_frames["rs_6m"].at[trade_date, stock_id]

                if pd.notna(r1):
                    batch.append((
                        int(stock_id), td_str,
                        int(r1), int(r3) if pd.notna(r3) else None,
                        int(r6) if pd.notna(r6) else None,
                    ))

        if batch:
            cursor.executemany(insert_sql, batch)

            # JOIN UPDATE (한번에 업데이트)
            cursor.execute("""
                UPDATE bs_daily_prices p
                JOIN _tmp_rs t ON p.stock_id = t.stock_id AND p.trade_date = t.trade_date
                SET p.rs_1m = t.rs_1m, p.rs_3m = t.rs_3m, p.rs_6m = t.rs_6m
            """)
            update_count += cursor.rowcount

        cursor.execute("DROP TEMPORARY TABLE IF EXISTS _tmp_rs")
        conn.commit()

        done = min(chunk_start + CHUNK_DAYS, total_dates)
        print(f"  진행: {done}/{total_dates}일 ({update_count:,}행)", flush=True)

    print(f"[RS] 완료: {update_count:,}행 업데이트")
