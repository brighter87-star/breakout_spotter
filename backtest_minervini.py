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

인터랙티브 모드: 데이터 1회 로드 후 파라미터 변경하며 반복 테스트 가능.
"""

import pymysql
from collections import defaultdict
from multiprocessing import Pool, cpu_count
import time as time_mod
from db.connection import get_connection

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

WEEK52_DAYS = 252
MIN_HISTORY = WEEK52_DAYS + 10

# 변경 가능한 파라미터 (모듈 레벨 — multiprocessing fork 시 자식에게 복사됨)
_cfg = {
    "rs_threshold": 70,
    "volume_ratio_min": 2.0,
    "breakout_lookback": 60,
    "volume_avg_days": 20,
    "stop_loss_pct": 7.0,
    "trailing_stop_pct": 20.0,
    "ma200_trend_days": 20,
    "mcap_min": 1_000_000_000,
}

BT_START = "2016-01-01"

RS_MAP = {
    "1m": IDX_RS1M,
    "3m": IDX_RS3M,
    "6m": IDX_RS6M,
}


def check_minervini_template(prices, idx, rs_col_idx):
    """미너비니 트렌드 템플릿 8가지 조건 확인."""
    if idx < MIN_HISTORY:
        return False

    p = prices[idx]
    close = float(p[IDX_CLOSE])
    ma50 = p[IDX_MA50]
    ma150 = p[IDX_MA150]
    ma200 = p[IDX_MA200]
    rs_val = p[rs_col_idx]

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

    # 3. MA200 상승 추세
    trend_days = _cfg["ma200_trend_days"]
    if idx < trend_days:
        return False
    ma200_prev = prices[idx - trend_days][IDX_MA200]
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

    # 8. RS >= threshold
    if rs_val < _cfg["rs_threshold"]:
        return False

    return True


def check_buy_trigger(prices, idx):
    """매수 타이밍: 고가 돌파 + 거래량 배수."""
    lookback = _cfg["breakout_lookback"]
    vol_days = _cfg["volume_avg_days"]

    if idx < lookback or idx < vol_days:
        return False

    close = float(prices[idx][IDX_CLOSE])
    today_vol = prices[idx][IDX_VOL]
    if today_vol is None or today_vol <= 0:
        return False
    today_vol = int(today_vol)

    # 고가 돌파
    prev_high = max(float(prices[i][IDX_HIGH]) for i in range(idx - lookback, idx))
    if close <= prev_high:
        return False

    # 거래량 조건
    vol_sum = sum(int(prices[i][IDX_VOL] or 0) for i in range(idx - vol_days, idx))
    avg_vol = vol_sum / vol_days
    if avg_vol <= 0 or today_vol < avg_vol * _cfg["volume_ratio_min"]:
        return False

    return True


def backtest_stock(args):
    """단일 종목 백테스트."""
    stock_id, prices, start_idx, rs_col_idx = args
    trades = []
    n = len(prices)
    i = start_idx

    sl_pct = _cfg["stop_loss_pct"]
    trail_pct = _cfg["trailing_stop_pct"]

    while i < n:
        if not check_minervini_template(prices, i, rs_col_idx):
            i += 1
            continue
        if not check_buy_trigger(prices, i):
            i += 1
            continue

        entry_idx = i
        entry_price = float(prices[i][IDX_CLOSE])
        entry_date = str(prices[i][IDX_DATE])
        peak_price = entry_price
        stop_price = entry_price * (1 - sl_pct / 100)

        exit_price = None
        exit_date = None
        exit_reason = None

        for j in range(i + 1, n):
            close = float(prices[j][IDX_CLOSE])

            if close > peak_price:
                peak_price = close

            if close <= stop_price:
                exit_price = close
                exit_date = str(prices[j][IDX_DATE])
                exit_reason = "stop_loss"
                i = j + 1
                break

            if close <= peak_price * (1 - trail_pct / 100):
                exit_price = close
                exit_date = str(prices[j][IDX_DATE])
                exit_reason = "trailing_stop"
                i = j + 1
                break
        else:
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
# 실적 분류
# =====================================================================

def classify_earnings(stock_id, signal_date, earnings_map):
    """1=Growth, 2=4Q Loss, 0=미분류"""
    records = earnings_map.get(stock_id, [])
    if not records:
        return 0

    past = [r for r in records if str(r[0]) <= signal_date]
    if len(past) < 5:
        return 0

    last_4 = past[-4:]
    if all(r[1] is not None and r[1] < 0 for r in last_4):
        return 2

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
# 통계 + 출력
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
        "total": len(trades_list), "closed": n, "open": len(trades_list) - n,
        "wins": len(winners), "losses": len(losers),
        "win_rate": win_rate, "avg_ret": avg_ret,
        "avg_win": avg_win, "avg_loss": avg_loss,
        "pl_ratio": pl_ratio, "max_win": max_win, "max_loss": max_loss,
        "ev": ev, "avg_hold": avg_hold, "median_hold": median_hold,
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


def run_backtest_for_rs(stocks, bt_args_base, rs_col_idx, rs_label,
                        earnings_map, mcap_map, has_mcap_data):
    """특정 RS 기간으로 백테스트 실행 + 결과 출력."""
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
    mcap_min = _cfg["mcap_min"]
    filtered_trades = []
    mcap_filtered = 0

    for t in all_trades:
        sid = t["stock_id"]
        mcap_data = mcap_map.get(sid)
        if mcap_data and has_mcap_data:
            mcap = lookup_mcap(mcap_data[0], mcap_data[1], t["entry_date"])
            t["market_cap"] = mcap
            if mcap is not None and mcap < mcap_min:
                mcap_filtered += 1
                continue
        else:
            t["market_cap"] = None
        filtered_trades.append(t)

    if has_mcap_data and mcap_filtered:
        print(f"  Market cap filter: {len(all_trades)} -> {len(filtered_trades)} "
              f"(removed {mcap_filtered} < ${mcap_min/1e9:.0f}B)")
        all_trades = filtered_trades

    # 실적 분류
    for t in all_trades:
        t["earn_group"] = classify_earnings(t["stock_id"], t["entry_date"], earnings_map)

    # -- 결과 출력 --
    print(f"\n{'='*110}")
    print(f"  {rs_label}  ({len(all_trades)} trades)")
    print(f"  Config: RS>={_cfg['rs_threshold']}  Vol>={_cfg['volume_ratio_min']}x  "
          f"Breakout={_cfg['breakout_lookback']}d  SL={_cfg['stop_loss_pct']}%  Trail={_cfg['trailing_stop_pct']}%")
    print(f"{'='*110}")

    all_s = _calc_stats(all_trades)
    g1 = [t for t in all_trades if t["earn_group"] == 1]
    g2 = [t for t in all_trades if t["earn_group"] == 2]

    _print_table_header()
    _print_stat_row("ALL", all_s)
    _print_stat_row("Growth", _calc_stats(g1))
    _print_stat_row("4Q Loss", _calc_stats(g2))

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


def print_comparison(summary):
    """비교 요약표"""
    if not summary:
        return
    print(f"\n\n{'='*110}")
    print(f"  COMPARISON SUMMARY")
    print(f"{'='*110}")
    print(f"  {'Label':<30} {'N':>5}  {'Win%':>6}  {'AvgRet':>8}  {'AvgWin':>8}  "
          f"{'AvgLoss':>8}  {'P/L':>5}  {'EV':>7}  {'MaxWin':>8}  {'MaxLoss':>9}")
    print(f"  {'-'*108}")
    for label, s in summary:
        print(f"  {label:<30} {s['closed']:>5}  {s['win_rate']:>5.1f}%  "
              f"{s['avg_ret']:>+7.2f}%  {s['avg_win']:>+7.2f}%  {s['avg_loss']:>+7.2f}%  "
              f"{s['pl_ratio']:>5.2f}  {s['ev']:>+6.2f}%  "
              f"{s['max_win']:>+7.1f}%  {s['max_loss']:>+8.1f}%")


# =====================================================================
# 데이터 로드
# =====================================================================

def load_data():
    """DB에서 모든 데이터를 로드. 1회만 호출."""
    conn = get_connection()
    cur = conn.cursor()
    t0 = time_mod.time()

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
        FROM bs_earnings WHERE earnings_date >= '2014-01-01'
        ORDER BY stock_id, earnings_date
    """)
    earnings_map = defaultdict(list)
    for row in cur:
        earnings_map[row[0]].append((row[1], row[2], row[3]))
    print(f"  -> {len(earnings_map)} stocks earnings")

    print("[4/4] Loading historical market cap...", flush=True)
    cur.execute("""
        SELECT stock_id, trade_date, market_cap
        FROM bs_market_cap ORDER BY stock_id, trade_date
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

    return {
        "stocks": stocks,
        "bt_args_base": bt_args_base,
        "earnings_map": earnings_map,
        "mcap_map": mcap_map,
        "has_mcap_data": has_mcap_data,
    }


def run_default_tests(data):
    """기본 3개 RS 기간 테스트."""
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
            data["stocks"], data["bt_args_base"], rs_col_idx, rs_label,
            data["earnings_map"], data["mcap_map"], data["has_mcap_data"],
        )
        elapsed = time_mod.time() - t_start
        print(f"\n  [{rs_label}] completed in {elapsed:.0f}s")

        if stats:
            summary.append((rs_label, stats))

    print_comparison(summary)
    return summary


def interactive_mode(data):
    """인터랙티브 모드: 파라미터 변경 후 재테스트."""
    global _cfg
    all_summary = []

    print(f"\n{'='*110}")
    print("  INTERACTIVE MODE")
    print("  데이터가 메모리에 로드되어 있습니다. 파라미터를 변경하며 반복 테스트하세요.")
    print(f"{'='*110}")

    while True:
        print(f"\n  현재 설정:")
        print(f"    rs_threshold    = {_cfg['rs_threshold']}")
        print(f"    volume_ratio    = {_cfg['volume_ratio_min']}")
        print(f"    breakout_days   = {_cfg['breakout_lookback']}")
        print(f"    stop_loss       = {_cfg['stop_loss_pct']}%")
        print(f"    trailing_stop   = {_cfg['trailing_stop_pct']}%")
        print(f"    mcap_min        = ${_cfg['mcap_min']/1e9:.0f}B")
        print()
        print("  명령어:")
        print("    set <key> <value>   — 파라미터 변경 (예: set rs_threshold 80)")
        print("    run <rs_period>     — 테스트 실행 (1m / 3m / 6m / all)")
        print("    compare             — 지금까지 결과 비교표")
        print("    clear               — 비교표 초기화")
        print("    q                   — 종료")
        print()

        try:
            cmd = input("  > ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not cmd:
            continue

        parts = cmd.split()
        action = parts[0].lower()

        if action == "q" or action == "quit" or action == "exit":
            break

        elif action == "set" and len(parts) == 3:
            key = parts[1]
            val_str = parts[2]
            if key in _cfg:
                try:
                    if isinstance(_cfg[key], float):
                        _cfg[key] = float(val_str)
                    elif isinstance(_cfg[key], int):
                        _cfg[key] = int(val_str)
                    print(f"    -> {key} = {_cfg[key]}")
                except ValueError:
                    print(f"    -> 잘못된 값: {val_str}")
            else:
                print(f"    -> 알 수 없는 키: {key}")
                print(f"       사용 가능: {', '.join(_cfg.keys())}")

        elif action == "run" and len(parts) >= 2:
            period = parts[1].lower()
            if period == "all":
                rs_list = [("1m", IDX_RS1M), ("3m", IDX_RS3M), ("6m", IDX_RS6M)]
            elif period in RS_MAP:
                rs_list = [(period, RS_MAP[period])]
            else:
                print(f"    -> 잘못된 RS 기간: {period} (1m / 3m / 6m / all)")
                continue

            for rs_name, rs_col_idx in rs_list:
                label = (f"RS {rs_name.upper()} "
                         f"(rs>={_cfg['rs_threshold']} vol>={_cfg['volume_ratio_min']}x "
                         f"bo={_cfg['breakout_lookback']}d)")
                t_start = time_mod.time()
                stats = run_backtest_for_rs(
                    data["stocks"], data["bt_args_base"], rs_col_idx, label,
                    data["earnings_map"], data["mcap_map"], data["has_mcap_data"],
                )
                elapsed = time_mod.time() - t_start
                print(f"\n  Completed in {elapsed:.0f}s")
                if stats:
                    all_summary.append((label, stats))

        elif action == "compare":
            print_comparison(all_summary)

        elif action == "clear":
            all_summary.clear()
            print("    -> 비교표 초기화됨")

        else:
            print(f"    -> 알 수 없는 명령: {cmd}")


def main():
    print("=" * 110)
    print("Minervini Trend Template Backtest")
    print(f"Period: {BT_START} ~ present")
    print("=" * 110)
    print()

    data = load_data()

    # 기본 3개 RS 테스트
    run_default_tests(data)

    # 인터랙티브 모드
    interactive_mode(data)


if __name__ == "__main__":
    main()
