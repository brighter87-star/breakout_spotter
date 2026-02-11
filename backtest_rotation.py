"""
산업 로테이션 백테스트 (Industry Rotation).

전략:
- 매일 RS 1M >= 70인 종목이 가장 많은 industry 순위를 매김
- 1위 산업 3종목, 2위 2종목, 3위 1종목 = 총 6 포지션 (동일 비중)
- 상위 산업 구성(순서 포함)이 바뀌면 전량 매도 → 새 배분으로 재매수

사용법:
    python backtest_rotation.py
"""

from collections import defaultdict
import time as time_mod
from db.connection import get_connection

# 튜플 인덱스 (bs_daily_prices 쿼리 결과)
IDX_DATE = 0
IDX_OPEN = 1
IDX_HIGH = 2
IDX_LOW = 3
IDX_CLOSE = 4
IDX_VOL = 5
IDX_MA50 = 6
IDX_MA150 = 7
IDX_MA200 = 8
IDX_RS1M = 9
IDX_RS3M = 10
IDX_RS6M = 11

RS_MAP = {"1m": IDX_RS1M, "3m": IDX_RS3M, "6m": IDX_RS6M}

BT_START = "2016-01-01"
MIN_HISTORY = 262  # 252 + 여유


def load_data():
    """주가 + 종목(industry) 로드. 실적/시총 불필요."""
    conn = get_connection()
    cur = conn.cursor()
    t0 = time_mod.time()

    print("[1/2] Loading prices (with RS)...", flush=True)
    cur.execute("""
        SELECT stock_id, trade_date, open_price, high_price, low_price, close_price, volume,
               ma50, ma150, ma200, rs_1m, rs_3m, rs_6m
        FROM bs_daily_prices
        ORDER BY stock_id, trade_date
    """)
    all_prices = defaultdict(list)
    for row in cur:
        all_prices[row[0]].append(row[1:])
    t1 = time_mod.time()
    total_candles = sum(len(v) for v in all_prices.values())
    print(f"  -> {len(all_prices)} stocks, {total_candles:,} candles ({t1-t0:.0f}s)")

    print("[2/2] Loading stocks...", flush=True)
    cur.execute("SELECT id, ticker, name, industry FROM bs_stocks ORDER BY id")
    stocks = {r[0]: {"ticker": r[1], "name": r[2], "industry": r[3]} for r in cur.fetchall()}

    conn.close()

    # bt_args_base 준비
    bt_args_base = []
    for stock_id, prices in all_prices.items():
        start_idx = -1
        for idx, p in enumerate(prices):
            if str(p[IDX_DATE]) >= BT_START:
                start_idx = idx
                break
        if start_idx < 0:
            continue
        start_idx = max(MIN_HISTORY, start_idx)
        if start_idx >= len(prices) - 1:
            continue
        bt_args_base.append((stock_id, prices, start_idx))

    elapsed = time_mod.time() - t0
    print(f"\nData loaded in {elapsed:.0f}s. Eligible stocks: {len(bt_args_base)}\n")

    return {"stocks": stocks, "bt_args_base": bt_args_base}


def run_industry_rotation(data, rs_period="1m", threshold=70, alloc=None):
    """
    Industry rotation backtest — 다중 산업 배분.

    매일 RS >= threshold인 종목 수 기준으로 산업 순위를 매기고,
    상위 산업별로 alloc 만큼 종목을 보유.
    기본: 1위 산업 3종목, 2위 2종목, 3위 1종목 = 총 6 포지션.
    상위 산업 구성이 바뀌면 전량 매도 후 재배분.
    """
    if alloc is None:
        alloc = [3, 2, 1]

    n_slots = len(alloc)
    total_positions = sum(alloc)

    stocks_info = data["stocks"]
    bt_args = data["bt_args_base"]
    rs_col = RS_MAP[rs_period]

    # Filter to stocks with industry
    eligible = []
    for stock_id, prices, start_idx in bt_args:
        info = stocks_info.get(stock_id, {})
        industry = info.get("industry")
        if not industry:
            continue
        eligible.append((stock_id, industry, prices, start_idx))

    print(f"  Eligible stocks with industry: {len(eligible)}")
    if not eligible:
        print("  No stocks with industry data!")
        return None

    # Collect all unique dates
    print("  Collecting dates...", flush=True)
    date_set = set()
    for _, _, prices, start_idx in eligible:
        for i in range(start_idx, len(prices)):
            date_set.add(prices[i][IDX_DATE])
    dates = sorted(date_set)
    print(f"  Trading days: {len(dates)} ({dates[0]} ~ {dates[-1]})")

    # Build stock_id -> eligible index
    sid_to_eidx = {eligible[j][0]: j for j in range(len(eligible))}

    # Initialize pointers
    ptrs = [start_idx for _, _, _, start_idx in eligible]

    # Portfolio state
    INITIAL_CAPITAL = 100000.0
    capital = INITIAL_CAPITAL
    current_top = []
    holdings = []
    trades_log = []
    rotations_log = []
    daily_values = []

    n_eligible = len(eligible)
    n_dates = len(dates)

    alloc_str = "+".join(str(a) for a in alloc)
    print(f"  Alloc: {alloc_str} = {total_positions} positions across {n_slots} industries")
    print(f"  Running rotation...", flush=True)

    for di, dt in enumerate(dates):
        # Advance pointers
        for j in range(n_eligible):
            p_list = eligible[j][2]
            while ptrs[j] < len(p_list) and p_list[ptrs[j]][IDX_DATE] < dt:
                ptrs[j] += 1

        # Update holdings' last prices
        for h in holdings:
            j = sid_to_eidx.get(h["stock_id"])
            if j is None:
                continue
            p_list = eligible[j][2]
            if ptrs[j] < len(p_list) and p_list[ptrs[j]][IDX_DATE] == dt:
                h["last_price"] = float(p_list[ptrs[j]][IDX_CLOSE])

        # Count per-industry RS >= threshold
        industry_counts = defaultdict(int)
        industry_rs_sum = defaultdict(float)
        industry_stocks_today = defaultdict(list)

        for j in range(n_eligible):
            p_list = eligible[j][2]
            if ptrs[j] >= len(p_list) or p_list[ptrs[j]][IDX_DATE] != dt:
                continue

            p = p_list[ptrs[j]]
            rs_val = p[rs_col]
            if rs_val is None:
                continue

            rs_int = int(rs_val)
            if rs_int >= threshold:
                industry = eligible[j][1]
                industry_counts[industry] += 1
                industry_rs_sum[industry] += rs_int
                industry_stocks_today[industry].append(
                    (eligible[j][0], rs_int, float(p[IDX_CLOSE]))
                )

        if not industry_counts:
            if holdings:
                total_val = sum(h["last_price"] * h["shares"] for h in holdings)
            else:
                total_val = capital
            daily_values.append((dt, total_val))
            continue

        # Rank industries by (count desc, rs_sum desc)
        ranked = sorted(
            industry_counts.keys(),
            key=lambda ind: (industry_counts[ind], industry_rs_sum[ind]),
            reverse=True,
        )[:n_slots]

        # Rotation check
        if ranked != current_top:
            # Sell all
            if holdings:
                for h in holdings:
                    ret = (h["last_price"] / h["entry_price"] - 1) * 100
                    trades_log.append({
                        "stock_id": h["stock_id"],
                        "industry": h["industry"],
                        "rank": h["rank"],
                        "entry_date": str(h["entry_date"]),
                        "exit_date": str(dt),
                        "entry_price": h["entry_price"],
                        "exit_price": h["last_price"],
                        "return_pct": ret,
                    })
                capital = sum(h["last_price"] * h["shares"] for h in holdings)
                holdings = []

            rotations_log.append((str(dt), list(current_top), list(ranked)))

            # Buy: alloc[i] stocks from ranked[i] industry
            actual_positions = 0
            buy_plan = []
            for rank_idx, ind in enumerate(ranked):
                n_stocks = alloc[rank_idx]
                candidates = industry_stocks_today.get(ind, [])
                candidates.sort(key=lambda x: x[1], reverse=True)
                picks = candidates[:n_stocks]
                for stock_id, rs_val, close in picks:
                    buy_plan.append((stock_id, close, ind, rank_idx + 1))
                    actual_positions += 1

            if actual_positions > 0:
                per_position = capital / actual_positions
                for stock_id, close, ind, rank in buy_plan:
                    holdings.append({
                        "stock_id": stock_id,
                        "entry_price": close,
                        "entry_date": dt,
                        "shares": per_position / close,
                        "last_price": close,
                        "industry": ind,
                        "rank": rank,
                    })

            current_top = ranked

        # Track daily value
        if holdings:
            total_val = sum(h["last_price"] * h["shares"] for h in holdings)
        else:
            total_val = capital
        daily_values.append((dt, total_val))

        # Progress
        if (di + 1) % 500 == 0:
            print(f"  진행: {di+1}/{n_dates} (rotations: {len(rotations_log)}, "
                  f"capital: ${total_val:,.0f})", flush=True)

    # Close remaining holdings
    if holdings:
        for h in holdings:
            ret = (h["last_price"] / h["entry_price"] - 1) * 100
            trades_log.append({
                "stock_id": h["stock_id"],
                "industry": h["industry"],
                "rank": h["rank"],
                "entry_date": str(h["entry_date"]),
                "exit_date": str(dates[-1]),
                "entry_price": h["entry_price"],
                "exit_price": h["last_price"],
                "return_pct": ret,
            })
        capital = sum(h["last_price"] * h["shares"] for h in holdings)

    final_value = capital
    total_return = (final_value / INITIAL_CAPITAL - 1) * 100
    n_years = (dates[-1] - dates[0]).days / 365.25
    cagr = ((final_value / INITIAL_CAPITAL) ** (1 / n_years) - 1) * 100 if n_years > 0 else 0

    # Max drawdown
    peak = 0
    max_dd = 0
    for _, val in daily_values:
        if val > peak:
            peak = val
        dd = (peak - val) / peak * 100 if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

    # ---- 결과 출력 ----
    print(f"\n{'='*110}")
    print(f"  INDUSTRY ROTATION BACKTEST")
    print(f"  RS: {rs_period.upper()} >= {threshold},  Alloc: {alloc_str} ({total_positions} positions)")
    print(f"  Period: {dates[0]} ~ {dates[-1]} ({n_years:.1f} years)")
    print(f"{'='*110}")
    print(f"  Initial:      ${INITIAL_CAPITAL:>12,.0f}")
    print(f"  Final:        ${final_value:>12,.0f}")
    print(f"  Total return: {total_return:>+10.1f}%")
    print(f"  CAGR:         {cagr:>+10.1f}%")
    print(f"  Max drawdown: {max_dd:>10.1f}%")
    print(f"  Rotations:    {len(rotations_log):>10}")
    print(f"  Total trades: {len(trades_log):>10}")

    win_rate = 0
    if trades_log:
        wins = [t for t in trades_log if t["return_pct"] > 0]
        losses = [t for t in trades_log if t["return_pct"] <= 0]
        avg_ret = sum(t["return_pct"] for t in trades_log) / len(trades_log)
        win_rate = len(wins) / len(trades_log) * 100
        avg_win = sum(t["return_pct"] for t in wins) / len(wins) if wins else 0
        avg_loss = sum(t["return_pct"] for t in losses) / len(losses) if losses else 0

        print(f"\n  Per-trade stats:")
        print(f"    Win rate:   {win_rate:>6.1f}%")
        print(f"    Avg return: {avg_ret:>+6.2f}%")
        print(f"    Avg win:    {avg_win:>+6.2f}%")
        print(f"    Avg loss:   {avg_loss:>+6.2f}%")

    # Recent rotations
    print(f"\n  Recent 20 rotations:")
    print(f"  {'Date':>10}  {'#1':<25} {'#2':<25} {'#3':<25}")
    print(f"  {'-'*88}")
    for dt_str, old_top, new_top in rotations_log[-20:]:
        cols = []
        for i in range(n_slots):
            cols.append(new_top[i][:24] if i < len(new_top) else "-")
        print(f"  {dt_str:>10}  {cols[0]:<25} {cols[1] if len(cols)>1 else '-':<25} "
              f"{cols[2] if len(cols)>2 else '-':<25}")

    # Top industries by appearance frequency
    ind_appear = defaultdict(int)
    for _, _, new_top in rotations_log:
        for ind in new_top:
            ind_appear[ind] += 1
    top_industries = sorted(ind_appear.items(), key=lambda x: x[1], reverse=True)[:15]
    total_rot = max(len(rotations_log), 1)
    print(f"\n  Top industries by appearance frequency:")
    print(f"  {'Industry':<40}  {'Times':>5}  {'Pct':>5}")
    print(f"  {'-'*55}")
    for ind, cnt in top_industries:
        print(f"  {ind:<40}  {cnt:>5}  {cnt/total_rot*100:>4.1f}%")

    # Per-industry performance
    ind_trades = defaultdict(list)
    for t in trades_log:
        ind_trades[t["industry"]].append(t)

    print(f"\n  Per-industry performance (top 15 by trade count):")
    print(f"  {'Industry':<35}  {'N':>4}  {'Win%':>5}  {'AvgRet':>7}  {'TotalRet':>8}")
    print(f"  {'-'*65}")
    sorted_ind = sorted(ind_trades.items(), key=lambda x: len(x[1]), reverse=True)[:15]
    for ind, tlist in sorted_ind:
        n_trades = len(tlist)
        w = sum(1 for t in tlist if t["return_pct"] > 0)
        avg_r = sum(t["return_pct"] for t in tlist) / n_trades
        total_r = sum(t["return_pct"] for t in tlist)
        wr = w / n_trades * 100 if n_trades > 0 else 0
        print(f"  {ind:<35}  {n_trades:>4}  {wr:>4.1f}%  {avg_r:>+6.2f}%  {total_r:>+7.1f}%")

    # Per-rank performance
    print(f"\n  Per-rank performance:")
    print(f"  {'Rank':<10}  {'N':>4}  {'Win%':>5}  {'AvgRet':>7}")
    print(f"  {'-'*32}")
    for r in range(1, n_slots + 1):
        rank_trades = [t for t in trades_log if t["rank"] == r]
        if not rank_trades:
            continue
        n_t = len(rank_trades)
        w = sum(1 for t in rank_trades if t["return_pct"] > 0)
        avg_r = sum(t["return_pct"] for t in rank_trades) / n_t
        wr = w / n_t * 100
        n_alloc = alloc[r - 1]
        print(f"  #{r} ({n_alloc}종목)  {n_t:>4}  {wr:>4.1f}%  {avg_r:>+6.2f}%")

    # Recent trades
    print(f"\n  Recent 12 trades:")
    print(f"  {'Ticker':<7} {'Industry':<25} {'Rk':>2} {'Entry':>10} {'Exit':>10} {'Ret%':>7}")
    print(f"  {'-'*68}")
    for t in trades_log[-12:]:
        ticker = stocks_info.get(t["stock_id"], {}).get("ticker", "?")
        ind_short = (t["industry"] or "?")[:24]
        print(f"  {ticker:<7} {ind_short:<25} #{t['rank']} {t['entry_date']:>10} "
              f"{t['exit_date']:>10} {t['return_pct']:>+6.1f}%")

    return {
        "total_return": total_return,
        "cagr": cagr,
        "max_dd": max_dd,
        "rotations": len(rotations_log),
        "trades": len(trades_log),
        "win_rate": win_rate,
    }


def main():
    print("=" * 110)
    print("Industry Rotation Backtest")
    print(f"Period: {BT_START} ~ present")
    print("=" * 110)
    print()

    data = load_data()
    run_industry_rotation(data, rs_period="1m", threshold=70, alloc=[3, 2, 1])


if __name__ == "__main__":
    main()
