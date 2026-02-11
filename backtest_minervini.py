"""
미너비니 트렌드 템플릿 백테스트.

매수 조건 (Minervini Trend Template — 8가지 모두 충족):
1. 종가 > MA150 AND 종가 > MA200
2. MA150 > MA200
3. MA200 상승 추세 (20거래일 전 대비 상승)
4. MA50 > MA150 AND MA50 > MA200
5. 종가 > MA50
6. 종가 >= 52주 저가 × 1.30 (30%+ 위)
7. 종가 >= 52주 고가 × 0.75 (25% 이내)
8. RS >= 70 (1개월 / 3개월 / 6개월 각각 테스트)

매수 타이밍:
- 최근 60거래일(3개월) 고가 돌파
- 돌파일 거래량 >= 최근 20거래일 평균 거래량 × 2

매도 조건:
- 종가 기준 진입가 -7% → 손절
- 종가 기준 고점 대비 -20% → 트레일링 스탑

필터:
- 매수 시점 시가총액 $1B 이상 (bs_market_cap 데이터)

기간: DB 내 최대 기간 (2016~ 현재)
"""

import pymysql
from collections import defaultdict
from multiprocessing import Pool, cpu_count
import time as time_mod
from db.connection import get_connection

# -- 매수 파라미터 --
WEEK52_DAYS = 252
MA200_TREND_DAYS = 20       # MA200 상승 추세 확인 기간
BREAKOUT_LOOKBACK = 60      # 3개월 고가 돌파 기준
VOLUME_AVG_DAYS = 20        # 거래량 평균 기간
VOLUME_RATIO_MIN = 2.0      # 돌파일 거래량 배수
RS_THRESHOLD = 70           # RS 최소값

# -- 매도 파라미터 --
STOP_LOSS_PCT = 7.0
TRAILING_STOP_PCT = 20.0

# -- 시가총액 필터 --
MCAP_MIN = 1_000_000_000   # $1B

# -- 백테스트 기간 --
BT_START = "2016-01-01"

# 튜플 인덱스
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

# 시작 전 필요한 최소 데이터 수 (52주 + 여유)
MIN_HISTORY = WEEK52_DAYS + 10


def check_minervini_template(prices, idx, rs_col_idx):
    """미너비니 트렌드 템플릿 8가지 조건 확인. True = 후보 종목."""
    if idx < MIN_HISTORY:
        return False

    p = prices[idx]
    close = float(p[IDX_CLOSE])
    ma50 = p[IDX_MA50]
    ma150 = p[IDX_MA150]
    ma200 = p[IDX_MA200]
    rs_val = p[rs_col_idx]

    # MA/RS 데이터 있어야 함
    if ma50 is None or ma150 is None or ma200 is None or rs_val is None:
        return False

    ma50 = float(ma50)
    ma150 = float(ma150)
    ma200 = float(ma200)
    rs_val = int(rs_val)

    # 1. 종가 > MA150 AND 종가 > MA200
    if close <= ma150 or close <= ma200:
        return False

    # 2. MA150 > MA200
    if ma150 <= ma200:
        return False

    # 3. MA200 상승 추세 (20거래일 전 대비)
    if idx < MA200_TREND_DAYS:
        return False
    ma200_prev = prices[idx - MA200_TREND_DAYS][IDX_MA200]
    if ma200_prev is None or ma200 <= float(ma200_prev):
        return False

    # 4. MA50 > MA150 AND MA50 > MA200
    if ma50 <= ma150 or ma50 <= ma200:
        return False

    # 5. 종가 > MA50
    if close <= ma50:
        return False

    # 6. 종가 >= 52주 저가 × 1.30
    start = max(0, idx - WEEK52_DAYS)
    week52_low = min(float(prices[i][IDX_LOW]) for i in range(start, idx + 1))
    if close < week52_low * 1.30:
        return False

    # 7. 종가 >= 52주 고가 × 0.75
    week52_high = max(float(prices[i][IDX_HIGH]) for i in range(start, idx + 1))
    if close < week52_high * 0.75:
        return False

    # 8. RS >= 70
    if rs_val < RS_THRESHOLD:
        return False

    return True


def check_buy_trigger(prices, idx):
    """매수 타이밍 확인: 3개월 고가 돌파 + 거래량 2배."""
    if idx < BREAKOUT_LOOKBACK:
        return False

    close = float(prices[idx][IDX_CLOSE])
    today_vol = prices[idx][IDX_VOL]
    if today_vol is None or today_vol <= 0:
        return False
    today_vol = int(today_vol)

    # 3개월 고가 돌파 (직전 60거래일의 고가 중 최대)
    prev_high = max(float(prices[i][IDX_HIGH]) for i in range(idx - BREAKOUT_LOOKBACK, idx))
    if close <= prev_high:
        return False

    # 거래량 >= 20거래일 평균 × 2
    vol_start = idx - VOLUME_AVG_DAYS
    if vol_start < 0:
        return False
    vol_sum = sum(int(prices[i][IDX_VOL] or 0) for i in range(vol_start, idx))
    avg_vol = vol_sum / VOLUME_AVG_DAYS
    if avg_vol <= 0 or today_vol < avg_vol * VOLUME_RATIO_MIN:
        return False

    return True


def backtest_stock(args):
    """단일 종목 백테스트. (stock_id, prices, start_idx, rs_col_idx) → trades[]"""
    stock_id, prices, start_idx, rs_col_idx = args
    trades = []
    n = len(prices)
    i = start_idx

    while i < n:
        # 진입 조건: 템플릿 + 트리거
        if not check_minervini_template(prices, i, rs_col_idx):
            i += 1
            continue
        if not check_buy_trigger(prices, i):
            i += 1
            continue

        # 진입
        entry_idx = i
        entry_price = float(prices[i][IDX_CLOSE])
        entry_date = str(prices[i][IDX_DATE])
        peak_price = entry_price
        stop_price = entry_price * (1 - STOP_LOSS_PCT / 100)

        exit_price = None
        exit_date = None
        exit_reason = None

        for j in range(i + 1, n):
            close = float(prices[j][IDX_CLOSE])

            if close > peak_price:
                peak_price = close

            # 1) 손절: 진입가 -7%
            if close <= stop_price:
                exit_price = close
                exit_date = str(prices[j][IDX_DATE])
                exit_reason = "stop_loss"
                i = j + 1
                break

            # 2) 트레일링: 고점 -20%
            if close <= peak_price * (1 - TRAILING_STOP_PCT / 100):
                exit_price = close
                exit_date = str(prices[j][IDX_DATE])
                exit_reason = "trailing_stop"
                i = j + 1
                break
        else:
            # 미청산
            exit_price = float(prices[-1][IDX_CLOSE])
            exit_date = str(prices[-1][IDX_DATE])
            exit_reason = "open"
            i = n

        ret_pct = (exit_price / entry_price - 1) * 100
        hold_days = (n - 1 - entry_idx) if exit_reason == "open" else (j - entry_idx)

        trades.append({
            "stock_id": stock_id,
            "entry_date": entry_date,
            "exit_date": exit_date,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "return_pct": ret_pct,
            "exit_reason": exit_reason,
            "hold_days": hold_days,
        })

    return trades


# =====================================================================
# 실적 분류 (backtest_custom.py 로직 재사용)
# =====================================================================

def classify_earnings(stock_id, signal_date, earnings_map):
    """시그널 날짜 기준 실적 분류. 1=Growth, 2=4Q Loss, 0=미분류"""
    records = earnings_map.get(stock_id, [])
    if not records:
        return 0

    past = [r for r in records if str(r[0]) <= signal_date]
    if len(past) < 5:
        return 0

    # 그룹 2: 4분기 연속 적자
    last_4 = past[-4:]
    if all(r[1] is not None and r[1] < 0 for r in last_4):
        return 2

    # 그룹 1: 매출 YoY +10% AND EPS YoY +10% (둘 다 양수)
    latest = past[-1]
    yoy_match = past[-5]
    latest_eps, latest_rev = latest[1], latest[2]
    yoy_eps, yoy_rev = yoy_match[1], yoy_match[2]

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
    """이진 탐색으로 entry_date 이전 가장 가까운 시가총액 조회."""
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
    return mcap_values[result_idx] if result_idx >= 0 else None


# =====================================================================
# 통계 계산
# =====================================================================

MCAP_RANGES = [
    (1e9,   10e9,  "$1B~$10B"),
    (10e9,  50e9,  "$10B~$50B"),
    (50e9,  100e9, "$50B~$100B"),
    (100e9, 500e9, "$100B~$500B"),
    (500e9, 1e12,  "$500B~$1T"),
    (1e12,  1e15,  "$1T+"),
]


def _calc_stats(trades_list):
    """거래 리스트 → 통계 dict"""
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
    pl_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else float("inf")
    max_win = max((t["return_pct"] for t in closed), default=0)
    max_loss = min((t["return_pct"] for t in closed), default=0)

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
        "max_win": max_win,
        "max_loss": max_loss,
        "ev": ev,
        "avg_hold": avg_hold,
        "median_hold": median_hold,
    }


def _print_stat_row(label, s):
    if s is None:
        return
    print(f"  {label:<14} {s['closed']:>5}  {s['win_rate']:>5.1f}%  "
          f"{s['avg_ret']:>+7.2f}%  {s['avg_win']:>+7.2f}%  {s['avg_loss']:>+7.2f}%  "
          f"{s['pl_ratio']:>5.2f}  {s['ev']:>+6.2f}%  {s['avg_hold']:>5.1f}d  {s['median_hold']:>4.0f}d")


def _print_table_header():
    print(f"  {'':14} {'N':>5}  {'Win%':>6}  {'AvgRet':>8}  {'AvgWin':>8}  {'AvgLoss':>8}  "
          f"{'P/L':>5}  {'EV':>7}  {'Hold':>6}  {'Med':>5}")
    print(f"  {'-'*94}")


def _bucket_by_mcap(trades):
    result = []
    for lo, hi, label in MCAP_RANGES:
        bucket = [t for t in trades if t.get("market_cap") and lo <= t["market_cap"] < hi]
        result.append((label, bucket))
    return result


def run_backtest_for_rs(all_prices, stocks, bt_args_base, rs_col_idx, rs_label,
                        earnings_map, mcap_map, has_mcap_data):
    """특정 RS 기간으로 백테스트 실행 + 결과 출력. Returns: stats dict"""

    # multiprocessing 인자에 rs_col_idx 추가
    bt_args = [(sid, prices, start_idx, rs_col_idx)
               for sid, prices, start_idx in bt_args_base]

    num_workers = max(1, cpu_count() - 1)

    all_trades = []
    with Pool(num_workers) as pool:
        results = pool.map(backtest_stock, bt_args, chunksize=50)
        for trades in results:
            all_trades.extend(trades)

    if not all_trades:
        print(f"\n  [{rs_label}] No trades found.")
        return None

    # 시가총액 필터
    filtered_trades = []
    mcap_filtered = 0

    for t in all_trades:
        sid = t["stock_id"]
        mcap_data = mcap_map.get(sid)
        if mcap_data and has_mcap_data:
            mcap = lookup_mcap(mcap_data[0], mcap_data[1], t["entry_date"])
            t["market_cap"] = mcap
            if mcap is not None and mcap < MCAP_MIN:
                mcap_filtered += 1
                continue
        else:
            t["market_cap"] = None

        filtered_trades.append(t)

    if has_mcap_data and mcap_filtered:
        print(f"  Market cap filter: {len(all_trades)} -> {len(filtered_trades)} "
              f"(removed {mcap_filtered} < $1B)")
        all_trades = filtered_trades

    # 실적 분류
    for t in all_trades:
        t["earn_group"] = classify_earnings(t["stock_id"], t["entry_date"], earnings_map)

    # -- 결과 출력 --
    print(f"\n{'='*110}")
    print(f"  {rs_label}  ({len(all_trades)} trades)")
    print(f"{'='*110}")

    all_s = _calc_stats(all_trades)
    g1 = [t for t in all_trades if t["earn_group"] == 1]
    g2 = [t for t in all_trades if t["earn_group"] == 2]

    _print_table_header()
    _print_stat_row("ALL", all_s)
    _print_stat_row("Growth", _calc_stats(g1))
    _print_stat_row("4Q Loss", _calc_stats(g2))

    # 시총 구간별
    mcap_buckets = _bucket_by_mcap(all_trades)
    for mcap_label, bucket in mcap_buckets:
        if not bucket:
            continue
        s = _calc_stats(bucket)
        if s and s["closed"] >= 5:
            print()
            print(f"  [{mcap_label}] ({len(bucket)} trades)")
            _print_table_header()
            _print_stat_row("ALL", s)
            _print_stat_row("Growth", _calc_stats([t for t in bucket if t["earn_group"] == 1]))
            _print_stat_row("4Q Loss", _calc_stats([t for t in bucket if t["earn_group"] == 2]))

    # 최근 거래
    print(f"\n  Recent 20 trades:")
    print(f"  {'Ticker':<7} {'Entry':>10} {'Exit':>10} {'EntryP':>8} {'ExitP':>8} "
          f"{'Ret%':>7} {'Days':>5} {'MCap($B)':>9} {'Reason':<12}")
    print(f"  {'-'*85}")
    recent = sorted(all_trades, key=lambda t: t["entry_date"], reverse=True)[:20]
    for t in recent:
        ticker = stocks.get(t["stock_id"], {}).get("ticker", "?")
        mcap_str = f"{t['market_cap']/1e9:.1f}" if t.get("market_cap") else "N/A"
        print(f"  {ticker:<7} {t['entry_date']:>10} {t['exit_date']:>10} "
              f"${t['entry_price']:>6.2f} ${t['exit_price']:>6.2f} "
              f"{t['return_pct']:>+6.1f}% {t['hold_days']:>4}d {mcap_str:>9} {t['exit_reason']}")

    return all_s


def main():
    conn = get_connection()
    cur = conn.cursor()
    t0 = time_mod.time()

    print("=" * 110)
    print("Minervini Trend Template Backtest")
    print(f"Period: {BT_START} ~ present")
    print(f"Entry : Trend Template (8 conditions) + 60d high breakout + vol {VOLUME_RATIO_MIN}x")
    print(f"Exit  : SL -{STOP_LOSS_PCT}% / Trail -{TRAILING_STOP_PCT}%")
    print(f"Filter: Market cap >= ${MCAP_MIN/1e9:.0f}B at entry")
    print(f"RS test: 1M / 3M / 6M (>= {RS_THRESHOLD})")
    print("=" * 110)
    print()

    # 1. 데이터 로드
    print("[1/4] Loading prices (with MA + RS)...", flush=True)
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

    print("[2/4] Loading stocks...", flush=True)
    cur.execute("SELECT id, ticker, name, market_cap FROM bs_stocks ORDER BY id")
    stocks = {r[0]: {"ticker": r[1], "name": r[2], "market_cap": r[3]} for r in cur.fetchall()}

    print("[3/4] Loading earnings...", flush=True)
    cur.execute("""
        SELECT stock_id, earnings_date, eps_actual, revenue_actual
        FROM bs_earnings
        WHERE earnings_date >= '2014-01-01'
        ORDER BY stock_id, earnings_date
    """)
    earnings_map = defaultdict(list)
    for row in cur:
        earnings_map[row[0]].append((row[1], row[2], row[3]))
    print(f"  -> {len(earnings_map)} stocks earnings")

    print("[4/4] Loading historical market cap...", flush=True)
    cur.execute("""
        SELECT stock_id, trade_date, market_cap
        FROM bs_market_cap
        ORDER BY stock_id, trade_date
    """)
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

    has_mcap_data = len(mcap_map) > 0
    if not has_mcap_data:
        print("  ** WARNING: No market cap data. $1B filter disabled. **")
    else:
        total_mcap_rows = sum(len(v[0]) for v in mcap_map.values())
        print(f"  -> {len(mcap_map)} stocks, {total_mcap_rows:,} rows")

    conn.close()
    t2 = time_mod.time()
    print(f"\nData loaded in {t2-t0:.0f}s. Running backtests...\n")

    # 백테스트 인자 준비 (종목별 시작 인덱스)
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

    print(f"Eligible stocks: {len(bt_args_base)}")

    # RS 기간별 백테스트
    rs_configs = [
        (IDX_RS1M, "RS 1M (rs_1m >= 70)"),
        (IDX_RS3M, "RS 3M (rs_3m >= 70)"),
        (IDX_RS6M, "RS 6M (rs_6m >= 70)"),
    ]

    summary = []
    for rs_col_idx, rs_label in rs_configs:
        t_start = time_mod.time()
        print(f"\n{'#'*110}")
        print(f"  Running: {rs_label}")
        print(f"{'#'*110}")

        stats = run_backtest_for_rs(
            all_prices, stocks, bt_args_base, rs_col_idx, rs_label,
            earnings_map, mcap_map, has_mcap_data,
        )
        elapsed = time_mod.time() - t_start
        print(f"\n  [{rs_label}] completed in {elapsed:.0f}s")

        if stats:
            summary.append((rs_label, stats))

    # 비교 요약
    if summary:
        print(f"\n\n{'='*110}")
        print(f"  COMPARISON SUMMARY")
        print(f"{'='*110}")
        print(f"  {'RS Period':<25} {'N':>5}  {'Win%':>6}  {'AvgRet':>8}  {'AvgWin':>8}  "
              f"{'AvgLoss':>8}  {'P/L':>5}  {'EV':>7}  {'MaxWin':>8}  {'MaxLoss':>9}")
        print(f"  {'-'*105}")
        for label, s in summary:
            print(f"  {label:<25} {s['closed']:>5}  {s['win_rate']:>5.1f}%  "
                  f"{s['avg_ret']:>+7.2f}%  {s['avg_win']:>+7.2f}%  {s['avg_loss']:>+7.2f}%  "
                  f"{s['pl_ratio']:>5.2f}  {s['ev']:>+6.2f}%  "
                  f"{s['max_win']:>+7.1f}%  {s['max_loss']:>+8.1f}%")

    total_time = time_mod.time() - t0
    print(f"\nTotal time: {total_time:.0f}s")


if __name__ == "__main__":
    main()
