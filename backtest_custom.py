"""
커스텀 돌파 백테스트 v4.

매수 조건:
1. 최근 2년 저점 대비 200%+ 상승 (3배)
2. 종가 신고가 후 10~130거래일 횡보 (신고가 갱신 없음, -50% 이상 하락 없음)
3. 20일 평균 거래량 2x+ 동반 종가 기준 신고가 돌파
4. 돌파 첫날 종가 매수

매도 조건:
- 종가 기준 진입가 -7% -> 손절
- 종가 기준 고점 대비 -20% -> 매도
- 종가 기준 20일 이동평균선 이하 -> 매도

필터:
- 매수 시점 시가총액 $1B 이상만 (bs_market_cap 데이터 사용)

그룹 분류 (실적 기준, 시그널 날짜 기준 point-in-time):
- Growth: 최근 분기 매출 YoY +10% AND EPS YoY +10% (둘 다 양수)
- 4Q Loss: 4분기 연속 EPS 적자

기간: DB 내 최대 기간 (2016~ 현재)
"""

import pymysql
from collections import defaultdict
from multiprocessing import Pool, cpu_count
import time as time_mod

DB_CONFIG = {
    "host": "localhost",
    "port": 3307,
    "user": "brighter87",
    "password": "!Wjd06Gns30",
    "database": "asset_us",
}

# -- 스캔 파라미터 --
RISE_MIN_PCT = 200.0
RISE_LOOKBACK_DAYS = 504  # 2년 (252 * 2 거래일)
CONSOL_MIN_DAYS = 10
CONSOL_MAX_DAYS = 130
CONSOL_MAX_DROP_PCT = 50.0
VOLUME_RATIO_MIN = 2.0
VOLUME_AVG_DAYS = 20

# -- 매도 파라미터 --
STOP_LOSS_PCT = 7.0
TRAILING_STOP_PCT = 20.0
MA_EXIT_DAYS = 20  # 20일선 이탈 매도

# -- 시가총액 필터 --
MCAP_MIN = 1_000_000_000  # $1B

# -- 백테스트 기간 --
BT_START = "2016-01-01"


def check_breakout(prices, today_idx):
    """돌파 조건 확인 (종가 기준 신고가 + 횡보 + 거래량)"""
    today_close = float(prices[today_idx][4])
    today_vol = prices[today_idx][5]

    if today_close <= 0 or today_vol <= 0:
        return None

    # 거래량 체크
    vol_window = prices[today_idx - VOLUME_AVG_DAYS:today_idx]
    if len(vol_window) < VOLUME_AVG_DAYS:
        return None
    avg_vol = sum(p[5] for p in vol_window) / VOLUME_AVG_DAYS
    if avg_vol <= 0:
        return None
    volume_ratio = today_vol / avg_vol
    if volume_ratio < VOLUME_RATIO_MIN:
        return None

    # 횡보 고점(peak) 찾기: 직전 130일 내 최고 종가
    peak_close = 0.0
    peak_idx = -1
    search_limit = min(CONSOL_MAX_DAYS, today_idx)
    for offset in range(1, search_limit + 1):
        idx = today_idx - offset
        c = float(prices[idx][4])
        if c > peak_close:
            peak_close = c
            peak_idx = idx

    if peak_idx < 0 or peak_close <= 0:
        return None

    # 돌파 확인
    if today_close <= peak_close:
        return None

    # 횡보 기간 확인
    consol_days = today_idx - peak_idx
    if consol_days < CONSOL_MIN_DAYS or consol_days > CONSOL_MAX_DAYS:
        return None

    # 횡보 중 -50% 하락 금지
    floor = peak_close * (1 - CONSOL_MAX_DROP_PCT / 100)
    for k in range(peak_idx + 1, today_idx):
        if float(prices[k][4]) < floor:
            return None

    # 첫날 확인: 전날 종가 <= peak
    if today_idx >= 1 and float(prices[today_idx - 1][4]) > peak_close:
        return None

    # 저점 대비 200% 상승 (최근 2년 내)
    if peak_idx < 10:
        return None
    low_search_start = max(0, peak_idx - RISE_LOOKBACK_DAYS)
    low_price = min(float(prices[k][3]) for k in range(low_search_start, peak_idx))
    if low_price <= 0:
        return None
    rise_pct = (peak_close / low_price - 1) * 100
    if rise_pct < RISE_MIN_PCT:
        return None

    return {"volume_ratio": volume_ratio, "rise_pct": rise_pct, "consol_days": consol_days}


def backtest_stock(args):
    """단일 종목 백테스트. (stock_id, prices, start_idx) -> trades[]"""
    stock_id, prices, start_idx = args
    trades = []
    n = len(prices)
    i = start_idx

    while i < n:
        result = check_breakout(prices, i)
        if not result:
            i += 1
            continue

        entry_idx = i
        entry_price = float(prices[i][4])
        entry_date = prices[i][0]
        peak_price = entry_price
        stop_price = entry_price * (1 - STOP_LOSS_PCT / 100)

        exit_price = None
        exit_date = None
        exit_reason = None

        for j in range(i + 1, n):
            close = float(prices[j][4])

            if close > peak_price:
                peak_price = close

            # 1) 손절: 진입가 -7%
            if close <= stop_price:
                exit_price = close
                exit_date = prices[j][0]
                exit_reason = "stop_loss"
                i = j + 1
                break

            # 2) 트레일링: 고점 -20%
            if close <= peak_price * (1 - TRAILING_STOP_PCT / 100):
                exit_price = close
                exit_date = prices[j][0]
                exit_reason = "trailing_stop"
                i = j + 1
                break

            # 3) 20일선 이탈: 종가 < 20일 이동평균
            if j >= MA_EXIT_DAYS:
                ma20 = sum(float(prices[j - k][4]) for k in range(MA_EXIT_DAYS)) / MA_EXIT_DAYS
                if close < ma20:
                    exit_price = close
                    exit_date = prices[j][0]
                    exit_reason = "ma20_break"
                    i = j + 1
                    break
        else:
            exit_price = float(prices[-1][4])
            exit_date = prices[-1][0]
            exit_reason = "open"
            i = n

        ret_pct = (exit_price / entry_price - 1) * 100

        if exit_reason == "open":
            hold_trading_days = (n - 1) - entry_idx
        else:
            hold_trading_days = j - entry_idx

        trades.append({
            "stock_id": stock_id,
            "entry_date": str(entry_date),
            "exit_date": str(exit_date),
            "entry_price": entry_price,
            "exit_price": exit_price,
            "return_pct": ret_pct,
            "exit_reason": exit_reason,
            "hold_days": hold_trading_days,
            "volume_ratio": result["volume_ratio"],
            "rise_pct": result["rise_pct"],
            "consol_days": result["consol_days"],
        })

    return trades


def classify_earnings(stock_id, signal_date, earnings_map):
    """
    시그널 날짜 기준 실적 분류.
    Returns: 1 (성장주), 2 (4분기 연속 적자), 0 (분류 불가)
    """
    records = earnings_map.get(stock_id, [])
    if not records:
        return 0

    past = [r for r in records if str(r[0]) <= signal_date]
    if len(past) < 5:
        return 0

    latest = past[-1]
    latest_eps = latest[1]
    latest_rev = latest[2]

    yoy_match = past[-5]
    yoy_eps = yoy_match[1]
    yoy_rev = yoy_match[2]

    # 그룹 2: 4분기 연속 적자
    last_4 = past[-4:]
    if all(r[1] is not None and r[1] < 0 for r in last_4):
        return 2

    # 그룹 1: 매출 YoY +10% AND EPS YoY +10% (둘 다 양수)
    if (latest_eps is not None and yoy_eps is not None and
        latest_rev is not None and yoy_rev is not None and
        latest_eps > 0 and yoy_eps > 0 and
        latest_rev > 0 and yoy_rev > 0):

        eps_growth = (latest_eps / yoy_eps - 1) * 100
        rev_growth = (latest_rev / yoy_rev - 1) * 100

        if eps_growth >= 10 and rev_growth >= 10:
            return 1

    return 0


def lookup_mcap(mcap_dates, mcap_values, entry_date_str):
    """
    이진 탐색으로 entry_date 이전 가장 가까운 시가총액 조회.
    mcap_dates: sorted list of date strings ("YYYY-MM-DD")
    mcap_values: parallel list of market cap values
    Returns: market cap (int) or None
    """
    if not mcap_dates:
        return None

    lo, hi = 0, len(mcap_dates) - 1
    result_idx = -1

    while lo <= hi:
        mid = (lo + hi) // 2
        if mcap_dates[mid] <= entry_date_str:
            result_idx = mid
            lo = mid + 1
        else:
            hi = mid - 1

    if result_idx >= 0:
        return mcap_values[result_idx]
    return None


# =====================================================================
# 통계 계산 유틸리티
# =====================================================================

def _calc_stats(trades_list):
    """거래 리스트 -> 통계 dict (청산된 건만)"""
    closed = [t for t in trades_list if t["exit_reason"] != "open"]
    if not closed:
        return None

    winners = [t for t in closed if t["return_pct"] > 0]
    losers = [t for t in closed if t["return_pct"] <= 0]

    n = len(closed)
    win_rate = len(winners) / n * 100
    avg_ret = sum(t["return_pct"] for t in closed) / n
    avg_win = sum(t["return_pct"] for t in winners) / len(winners) if winners else 0
    avg_loss = sum(t["return_pct"] for t in losers) / len(losers) if losers else 0
    pl_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')

    wr = len(winners) / n
    ev = (wr * avg_win) + ((1 - wr) * avg_loss)

    avg_hold = sum(t["hold_days"] for t in closed) / n
    median_hold = sorted(t["hold_days"] for t in closed)[n // 2]

    return {
        "total": len(trades_list),
        "closed": n,
        "open": len(trades_list) - n,
        "wins": len(winners),
        "losses": len(losers),
        "win_rate": win_rate,
        "avg_ret": avg_ret,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "pl_ratio": pl_ratio,
        "ev": ev,
        "avg_hold": avg_hold,
        "median_hold": median_hold,
    }


def _print_stat_row(label, s):
    """통계 한 줄 출력"""
    if s is None:
        return
    print(f"  {label:<14} {s['closed']:>5}  {s['win_rate']:>5.1f}%  "
          f"{s['avg_ret']:>+7.2f}%  {s['avg_win']:>+7.2f}%  {s['avg_loss']:>+7.2f}%  "
          f"{s['pl_ratio']:>5.2f}  {s['ev']:>+6.2f}%  {s['avg_hold']:>5.1f}d  {s['median_hold']:>4.0f}d")


def _print_table_header():
    """테이블 헤더"""
    print(f"  {'':14} {'N':>5}  {'Win%':>6}  {'AvgRet':>8}  {'AvgWin':>8}  {'AvgLoss':>8}  "
          f"{'P/L':>5}  {'EV':>7}  {'Hold':>6}  {'Med':>5}")
    print(f"  {'-'*94}")


def _get_price_quintiles(trades):
    """주가 기준 5분위 분류. Returns: [(label, trades_list), ...]"""
    closed = [t for t in trades if t["exit_reason"] != "open"]
    if len(closed) < 5:
        return []

    closed.sort(key=lambda t: t["entry_price"])
    n = len(closed)
    q_size = n // 5
    quintiles = []

    for q in range(5):
        start = q * q_size
        end = (q + 1) * q_size if q < 4 else n
        bucket = closed[start:end]
        lo = bucket[0]["entry_price"]
        hi = bucket[-1]["entry_price"]
        if hi >= 10000:
            label = f"Q{q+1} ${lo:.0f}+"
        else:
            label = f"Q{q+1} ${lo:.0f}~${hi:.0f}"
        quintiles.append((label, bucket))

    return quintiles


MCAP_RANGES = [
    (1e9,   10e9,  "$1B~$10B"),
    (10e9,  50e9,  "$10B~$50B"),
    (50e9,  100e9, "$50B~$100B"),
    (100e9, 500e9, "$100B~$500B"),
    (500e9, 1e12,  "$500B~$1T"),
    (1e12,  1e15,  "$1T+"),
]


def _bucket_by_mcap(trades):
    """시가총액 구간별 분류. Returns: [(label, trades_list), ...]"""
    result = []
    for lo, hi, label in MCAP_RANGES:
        bucket = [t for t in trades if t.get("market_cap") and lo <= t["market_cap"] < hi]
        result.append((label, bucket))
    return result


# =====================================================================
# 메인
# =====================================================================

def main():
    conn = pymysql.connect(**DB_CONFIG)
    cur = conn.cursor()

    t0 = time_mod.time()

    print("=" * 70)
    print("Custom Breakout Backtest v4")
    print(f"Period: {BT_START} ~ present")
    print(f"Entry : {RISE_MIN_PCT:.0f}%+ rise (2yr), {CONSOL_MIN_DAYS}-{CONSOL_MAX_DAYS}d consol, "
          f"{VOLUME_AVG_DAYS}d vol {VOLUME_RATIO_MIN}x+")
    print(f"Exit  : SL -{STOP_LOSS_PCT}% / Trail -{TRAILING_STOP_PCT}% / MA{MA_EXIT_DAYS} break")
    print(f"Filter: Market cap >= ${MCAP_MIN/1e9:.0f}B at entry")
    print("=" * 70)
    print()

    # 1. 데이터 로드
    print("[1/5] Loading stocks...", flush=True)
    cur.execute("SELECT id, ticker, name, market_cap FROM bs_stocks ORDER BY id")
    stocks = {r[0]: {"ticker": r[1], "name": r[2], "market_cap": r[3]} for r in cur.fetchall()}

    print("[2/5] Loading prices...", flush=True)
    cur.execute("""
        SELECT stock_id, trade_date, open_price, high_price, low_price, close_price, volume
        FROM bs_daily_prices
        ORDER BY stock_id, trade_date
    """)

    all_prices = defaultdict(list)
    for row in cur:
        all_prices[row[0]].append(row[1:])

    t1 = time_mod.time()
    total_candles = sum(len(v) for v in all_prices.values())
    print(f"  -> {len(all_prices)} stocks, {total_candles:,} candles ({t1-t0:.0f}s)")

    print("[3/5] Loading earnings...", flush=True)
    cur.execute("""
        SELECT stock_id, earnings_date, eps_actual, revenue_actual
        FROM bs_earnings
        WHERE earnings_date >= '2014-01-01'
        ORDER BY stock_id, earnings_date
    """)

    earnings_map = defaultdict(list)
    for row in cur:
        earnings_map[row[0]].append((row[1], row[2], row[3]))

    t2 = time_mod.time()
    print(f"  -> {len(earnings_map)} stocks earnings ({t2-t1:.0f}s)")

    print("[4/5] Loading historical market cap...", flush=True)
    cur.execute("""
        SELECT stock_id, trade_date, market_cap
        FROM bs_market_cap
        ORDER BY stock_id, trade_date
    """)

    # {stock_id: (dates_list, values_list)} - sorted by date for binary search
    mcap_map = {}
    current_sid = None
    dates_buf, vals_buf = [], []
    for row in cur:
        sid, dt, mcap = row
        if sid != current_sid:
            if current_sid is not None and dates_buf:
                mcap_map[current_sid] = ([str(d) for d in dates_buf], vals_buf)
            current_sid = sid
            dates_buf, vals_buf = [], []
        dates_buf.append(dt)
        vals_buf.append(mcap)
    if current_sid is not None and dates_buf:
        mcap_map[current_sid] = ([str(d) for d in dates_buf], vals_buf)

    t2b = time_mod.time()
    total_mcap_rows = sum(len(v[0]) for v in mcap_map.values())
    print(f"  -> {len(mcap_map)} stocks, {total_mcap_rows:,} rows ({t2b-t2:.0f}s)")

    has_mcap_data = len(mcap_map) > 0
    if not has_mcap_data:
        print("  ** WARNING: No market cap data. $1B filter disabled. **")

    conn.close()

    # 2. 백테스트
    print("[5/5] Running backtest...", flush=True)

    bt_args = []
    for stock_id, prices in all_prices.items():
        start_idx = -1
        for idx, p in enumerate(prices):
            if str(p[0]) >= BT_START:
                start_idx = idx
                break
        if start_idx < 0 or start_idx < CONSOL_MAX_DAYS + VOLUME_AVG_DAYS:
            start_idx = max(CONSOL_MAX_DAYS + VOLUME_AVG_DAYS, start_idx)
        if start_idx >= len(prices) - 1:
            continue
        bt_args.append((stock_id, prices, start_idx))

    num_workers = max(1, cpu_count() - 1)
    print(f"  -> {len(bt_args)} stocks, {num_workers} CPU cores")

    all_trades = []
    with Pool(num_workers) as pool:
        results = pool.map(backtest_stock, bt_args, chunksize=50)
        for trades in results:
            all_trades.extend(trades)

    t3 = time_mod.time()
    print(f"  -> {len(all_trades)} trades found ({t3-t2b:.0f}s)")

    if not all_trades:
        print("\nNo trades found.")
        return

    # 시가총액 조회 + $1B 필터
    filtered_trades = []
    mcap_filtered = 0
    mcap_missing = 0

    for t in all_trades:
        sid = t["stock_id"]
        mcap_data = mcap_map.get(sid)
        if mcap_data and has_mcap_data:
            mcap = lookup_mcap(mcap_data[0], mcap_data[1], t["entry_date"])
            t["market_cap"] = mcap
            if mcap is not None and mcap < MCAP_MIN:
                mcap_filtered += 1
                continue
            if mcap is None:
                mcap_missing += 1
        else:
            t["market_cap"] = None
            if has_mcap_data:
                mcap_missing += 1

        filtered_trades.append(t)

    if has_mcap_data:
        print(f"  -> Market cap filter: {len(all_trades)} -> {len(filtered_trades)} trades "
              f"(removed {mcap_filtered} < $1B, {mcap_missing} no data)")
        all_trades = filtered_trades

    # 3. 실적 분류
    print("\nClassifying by earnings...", flush=True)

    for t in all_trades:
        g = classify_earnings(t["stock_id"], t["entry_date"], earnings_map)
        t["earn_group"] = g  # 1=Growth, 2=4Q Loss, 0=unclassified

    t4 = time_mod.time()

    # =================================================================
    # 4. 결과 출력
    # =================================================================
    print()
    print("=" * 110)
    print(f"  BACKTEST RESULTS  ({BT_START} ~ present)")
    print(f"  Total time: {t4-t0:.0f}s  |  Filter: MCap >= ${MCAP_MIN/1e9:.0f}B")
    print("=" * 110)

    # 시총 구간별 분류
    mcap_buckets = _bucket_by_mcap(all_trades)

    # -- (A) 전체 요약 (시총 구간 무관) --
    print(f"\n{'='*110}")
    print(f"  OVERALL (all market cap ranges)")
    print(f"{'='*110}")

    all_s = _calc_stats(all_trades)
    g1_all = [t for t in all_trades if t["earn_group"] == 1]
    g2_all = [t for t in all_trades if t["earn_group"] == 2]

    _print_table_header()
    _print_stat_row("ALL", all_s)
    _print_stat_row("Growth", _calc_stats(g1_all))
    _print_stat_row("4Q Loss", _calc_stats(g2_all))

    n_g1 = len(g1_all)
    n_g2 = len(g2_all)
    n_other = len(all_trades) - n_g1 - n_g2
    print(f"\n  Trades: {len(all_trades)} total  (Growth={n_g1}, 4Q Loss={n_g2}, Other={n_other})")

    # -- (B) 시총 구간별 --
    for mcap_label, bucket in mcap_buckets:
        if not bucket:
            continue

        s = _calc_stats(bucket)
        if s is None:
            continue

        g1 = [t for t in bucket if t["earn_group"] == 1]
        g2 = [t for t in bucket if t["earn_group"] == 2]

        print(f"\n{'='*110}")
        print(f"  {mcap_label}  ({len(bucket)} trades)")
        print(f"{'='*110}")

        _print_table_header()
        _print_stat_row("ALL", s)
        _print_stat_row("Growth", _calc_stats(g1))
        _print_stat_row("4Q Loss", _calc_stats(g2))

    # -- (C) 분류 통계 요약 --
    print(f"\n{'='*110}")
    print(f"  SUMMARY BY MARKET CAP")
    print(f"{'='*110}")
    print(f"  {'MCap Range':<16} {'Total':>6} {'Growth':>7} {'4Q Loss':>8} {'Other':>7}")
    print(f"  {'-'*50}")
    for mcap_label, bucket in mcap_buckets:
        ng1 = len([t for t in bucket if t["earn_group"] == 1])
        ng2 = len([t for t in bucket if t["earn_group"] == 2])
        nother = len(bucket) - ng1 - ng2
        print(f"  {mcap_label:<16} {len(bucket):>6} {ng1:>7} {ng2:>8} {nother:>7}")
    print(f"  {'-'*50}")
    print(f"  {'TOTAL':<16} {len(all_trades):>6} {n_g1:>7} {n_g2:>8} {n_other:>7}")

    # -- (D) 최근 거래 --
    print(f"\n  Recent 20 trades:")
    print(f"  {'Ticker':<7} {'Entry':>10} {'Exit':>10} {'EntryP':>8} {'ExitP':>8} "
          f"{'Ret%':>7} {'MCap($B)':>9} {'Reason':<12}")
    print(f"  {'-'*80}")
    recent = sorted(all_trades, key=lambda t: t["entry_date"], reverse=True)[:20]
    for t in recent:
        ticker = stocks.get(t["stock_id"], {}).get("ticker", "?")
        mcap_str = f"{t['market_cap']/1e9:.1f}" if t.get("market_cap") else "N/A"
        print(f"  {ticker:<7} {t['entry_date']:>10} {t['exit_date']:>10} "
              f"${t['entry_price']:>6.2f} ${t['exit_price']:>6.2f} "
              f"{t['return_pct']:>+6.1f}% {mcap_str:>9} {t['exit_reason']}")


if __name__ == "__main__":
    main()
