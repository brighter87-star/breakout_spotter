"""
백테스트 엔진.

매일 돌파 스캔 → 섹터 그룹핑 → 상위 3섹터 각 1종목 선택 → 포지션 관리.

청산 규칙:
  1. 종가가 진입가 -7% 이하 → 손절 (종가로 청산)
  2. 고점 대비 -20% (종가 기준) → 트레일링 스탑
  재돌파 시 새 lot으로 매수 가능.

매수 필터: 시가총액 $10B ~ $500B (섹터 강도 계산은 전체 종목 대상)
"""

import pymysql
from collections import defaultdict
from config.settings import Settings
from services.breakout_scanner import check_breakout, score_breakout


STOP_LOSS_PCT = 7.0
TRAILING_STOP_PCT = 20.0
MCAP_MIN = 10_000_000_000    # $10B
MCAP_MAX = 500_000_000_000   # $500B


def load_all_prices(conn):
    """모든 주가 데이터를 메모리에 로드. {stock_id: [(date, o, h, l, c, v), ...]}"""
    cursor = conn.cursor()
    cursor.execute(
        """SELECT stock_id, trade_date, open_price, high_price, low_price, close_price, volume
           FROM bs_daily_prices ORDER BY stock_id, trade_date ASC"""
    )

    prices = defaultdict(list)
    for row in cursor.fetchall():
        sid = row[0]
        prices[sid].append((
            str(row[1]), float(row[2]), float(row[3]),
            float(row[4]), float(row[5]), int(row[6])
        ))

    return dict(prices)


def load_stock_info(conn):
    """종목 정보 로드. {stock_id: {ticker, sector, exchange_code, market_cap}}"""
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT id, ticker, sector, exchange_code, market_cap FROM bs_stocks WHERE is_active = 1")
    return {row["id"]: row for row in cursor.fetchall()}


def load_sectors_from_db(conn):
    """DB에 저장된 섹터 데이터 로드"""
    cursor = conn.cursor()
    cursor.execute("SELECT ticker, sector FROM bs_stocks WHERE sector IS NOT NULL AND sector != ''")
    return {row[0]: row[1] for row in cursor.fetchall()}


def load_spy_prices(conn):
    """SPY 가격 데이터 로드 (벤치마크용). {date_str: close_price}"""
    cursor = conn.cursor()
    cursor.execute(
        """SELECT dp.trade_date, dp.close_price
           FROM bs_daily_prices dp
           JOIN bs_stocks s ON dp.stock_id = s.id
           WHERE s.ticker = 'SPY'
           ORDER BY dp.trade_date ASC"""
    )
    return {str(row[0]): float(row[1]) for row in cursor.fetchall()}


def run_backtest(conn):
    settings = Settings()

    print("[backtest] loading data...")
    all_prices = load_all_prices(conn)
    stock_info = load_stock_info(conn)
    sector_db = load_sectors_from_db(conn)
    spy_prices = load_spy_prices(conn)

    # 거래일 목록 + 날짜→종목 역인덱스
    all_dates = set()
    stock_date_idx = {}
    date_to_sids = defaultdict(list)

    for sid, prices in all_prices.items():
        idx_map = {}
        for i, p in enumerate(prices):
            idx_map[p[0]] = i
            all_dates.add(p[0])
            date_to_sids[p[0]].append(sid)
        stock_date_idx[sid] = idx_map

    trade_dates = sorted(all_dates)
    min_data_len = settings.HIGH_BREAKOUT_DAYS + 10
    start_idx = min_data_len

    # stock_id → sector
    sid_sector = {}
    for sid, info in stock_info.items():
        ticker = info.get("ticker", "")
        if ticker in sector_db:
            sid_sector[sid] = sector_db[ticker]

    mcap_eligible = sum(1 for sid, info in stock_info.items()
                        if info.get("market_cap") and MCAP_MIN <= info["market_cap"] <= MCAP_MAX)
    print(f"  stocks: {len(all_prices)}, dates: {len(trade_dates)} ({trade_dates[0]} ~ {trade_dates[-1]})")
    print(f"  sectors loaded: {len(sid_sector)}, market cap $10B~$500B: {mcap_eligible}")

    # ── 백테스트 실행 (단일 패스) ──
    all_trades = []
    open_positions = {}   # key = f"{ticker}_{lot_id}" → position
    lot_counter = 0
    total_dates = len(trade_dates) - start_idx

    for di, current_date in enumerate(trade_dates[start_idx:], start_idx):
        if (di - start_idx) % 20 == 0:
            pct = (di - start_idx) / total_dates * 100
            print(f"  [{pct:5.1f}%] {current_date} | open: {len(open_positions)} | trades: {len(all_trades)}")

        # ── 1. 기존 포지션 청산 확인 ──
        closed_keys = []
        for key, pos in open_positions.items():
            sid = pos["stock_id"]
            if current_date not in stock_date_idx.get(sid, {}):
                continue

            idx = stock_date_idx[sid][current_date]
            today = all_prices[sid][idx]
            today_high, today_low, today_close = today[2], today[3], today[4]

            # 고점 업데이트 (종가 기준)
            if today_close > pos["peak"]:
                pos["peak"] = today_close

            exit_reason = None
            exit_price = today_close

            # 1. 손절: 종가가 진입가 -7% 이하
            stop_price = pos["entry_price"] * (1 - STOP_LOSS_PCT / 100)
            if today_close <= stop_price:
                exit_reason = "stop_loss"
                exit_price = today_close

            # 2. 트레일링 스탑: 고점 대비 -20% (종가 기준)
            elif pos["peak"] > 0:
                trail_price = pos["peak"] * (1 - TRAILING_STOP_PCT / 100)
                if today_close <= trail_price:
                    exit_reason = "trailing_stop"
                    exit_price = today_close

            pos["hold_days"] += 1

            if exit_reason:
                pnl_pct = ((exit_price / pos["entry_price"]) - 1) * 100
                all_trades.append({
                    "ticker": pos["ticker"],
                    "sector": pos["sector"],
                    "entry_date": pos["entry_date"],
                    "entry_price": pos["entry_price"],
                    "exit_date": current_date,
                    "exit_price": round(exit_price, 4),
                    "peak": pos["peak"],
                    "pnl_pct": round(pnl_pct, 2),
                    "hold_days": pos["hold_days"],
                    "exit_reason": exit_reason,
                    "score": pos["score"],
                })
                closed_keys.append(key)

        for key in closed_keys:
            del open_positions[key]

        # ── 2. 오늘의 돌파 신호 (역인덱스 활용) ──
        day_signals = []
        for sid in date_to_sids[current_date]:
            idx = stock_date_idx[sid][current_date]
            if idx < min_data_len:
                continue

            price_slice = all_prices[sid][:idx + 1]
            breakout = check_breakout(price_slice, settings)
            if breakout is None:
                continue

            info = stock_info.get(sid, {})
            ticker = info.get("ticker", "")
            sector = sid_sector.get(sid, "Unknown")
            score = score_breakout(breakout)

            mcap = info.get("market_cap")
            day_signals.append({
                "stock_id": sid,
                "ticker": ticker,
                "sector": sector,
                "score": score,
                "close_price": breakout["close_price"],
                "market_cap": mcap,
            })

        # ── 3. 섹터 그룹핑 + 상위 3섹터 선택 ──
        picks = select_top3_by_sector(day_signals)

        # ── 4. 신규 진입 (같은 종목이라도 기존 lot 청산 후 재진입 가능) ──
        open_tickers = {pos["ticker"] for pos in open_positions.values()}
        for pick in picks:
            ticker = pick["ticker"]
            if ticker in open_tickers:
                continue

            lot_counter += 1
            key = f"{ticker}_{lot_counter}"
            open_positions[key] = {
                "stock_id": pick["stock_id"],
                "ticker": ticker,
                "sector": pick["sector"],
                "entry_date": current_date,
                "entry_price": pick["close_price"],
                "peak": pick["close_price"],
                "score": pick["score"],
                "hold_days": 0,
            }

    # ── 미청산 포지션 마지막 날 종가로 정리 ──
    last_date = trade_dates[-1]
    for key, pos in open_positions.items():
        sid = pos["stock_id"]
        if last_date in stock_date_idx.get(sid, {}):
            idx = stock_date_idx[sid][last_date]
            last_close = all_prices[sid][idx][4]
        else:
            last_close = pos["entry_price"]

        pnl_pct = ((last_close / pos["entry_price"]) - 1) * 100
        all_trades.append({
            "ticker": pos["ticker"],
            "sector": pos["sector"],
            "entry_date": pos["entry_date"],
            "entry_price": pos["entry_price"],
            "exit_date": last_date,
            "exit_price": last_close,
            "peak": pos["peak"],
            "pnl_pct": round(pnl_pct, 2),
            "hold_days": pos["hold_days"],
            "exit_reason": "open",
            "score": pos["score"],
        })

    bt_start = trade_dates[start_idx]
    bt_end = trade_dates[-1]
    print_backtest_results(all_trades, spy_prices, bt_start, bt_end)
    return all_trades


def select_top3_by_sector(signals):
    """섹터별 그룹핑 → 상위 3섹터 → 각 최고 점수 종목 1개"""
    if not signals:
        return []

    sector_groups = defaultdict(list)
    for sig in signals:
        sector_groups[sig["sector"]].append(sig)

    sector_strength = {}
    for sector, sigs in sector_groups.items():
        if sector == "Unknown":
            continue
        avg_score = sum(s["score"] for s in sigs) / len(sigs)
        strength = len(sigs) * 0.4 + (avg_score / 100) * 0.6

        sector_strength[sector] = {
            "strength": strength,
            "best": max(sigs, key=lambda x: x["score"]),
        }

    top_sectors = sorted(sector_strength.items(), key=lambda x: x[1]["strength"], reverse=True)[:3]
    return [data["best"] for _, data in top_sectors]


def print_backtest_results(trades, spy_prices=None, bt_start=None, bt_end=None):
    if not trades:
        print("\n[backtest] no trades")
        return

    total = len(trades)
    closed = [t for t in trades if t["exit_reason"] != "open"]
    still_open = [t for t in trades if t["exit_reason"] == "open"]
    winners = [t for t in closed if t["pnl_pct"] > 0]
    losers = [t for t in closed if t["pnl_pct"] <= 0]

    win_rate = len(winners) / len(closed) * 100 if closed else 0
    avg_pnl = sum(t["pnl_pct"] for t in closed) / len(closed) if closed else 0
    avg_win = sum(t["pnl_pct"] for t in winners) / len(winners) if winners else 0
    avg_loss = sum(t["pnl_pct"] for t in losers) / len(losers) if losers else 0
    avg_hold = sum(t["hold_days"] for t in closed) / len(closed) if closed else 0
    max_win = max((t["pnl_pct"] for t in closed), default=0)
    max_loss = min((t["pnl_pct"] for t in closed), default=0)

    reasons = defaultdict(list)
    for t in closed:
        reasons[t["exit_reason"]].append(t["pnl_pct"])

    sector_stats = defaultdict(list)
    for t in closed:
        sector_stats[t["sector"]].append(t["pnl_pct"])

    # 누적 수익 (3등분 배분, 청산일 순)
    cum_return = 1.0
    for t in sorted(closed, key=lambda x: x["exit_date"]):
        cum_return *= (1 + t["pnl_pct"] / 100 / 3)

    # SPY 벤치마크 수익률
    spy_return_pct = None
    if spy_prices and bt_start and bt_end:
        spy_start = spy_prices.get(bt_start)
        spy_end = spy_prices.get(bt_end)
        if spy_start and spy_end:
            spy_return_pct = ((spy_end / spy_start) - 1) * 100

    print(f"\n{'='*70}")
    print(f"  BACKTEST RESULTS")
    print(f"{'='*70}")
    if bt_start and bt_end:
        print(f"  Period: {bt_start} ~ {bt_end}")
    print(f"  Closed trades: {len(closed)}  (still open: {len(still_open)})")
    print(f"  Win rate: {win_rate:.1f}% ({len(winners)}W / {len(losers)}L)")
    print(f"  Avg return: {avg_pnl:+.2f}%")
    print(f"  Avg win: {avg_win:+.2f}%  |  Avg loss: {avg_loss:+.2f}%")
    print(f"  Best: {max_win:+.2f}%  |  Worst: {max_loss:+.2f}%")
    print(f"  Avg hold: {avg_hold:.1f} days")
    if avg_loss != 0:
        print(f"  Win/Loss ratio: {abs(avg_win/avg_loss):.2f}")
    print(f"  Cumulative (1/3 alloc): {(cum_return - 1) * 100:+.2f}%")
    if spy_return_pct is not None:
        print(f"  SPY Buy & Hold (same period): {spy_return_pct:+.2f}%")
        strategy_pct = (cum_return - 1) * 100
        alpha = strategy_pct - spy_return_pct
        print(f"  Alpha vs SPY: {alpha:+.2f}%")

    print(f"\n  [By Exit Reason]")
    for reason, pnls in sorted(reasons.items()):
        avg = sum(pnls) / len(pnls)
        print(f"    {reason:15s}: {len(pnls):>3} trades, avg {avg:+.2f}%")

    print(f"\n  [By Sector]")
    for sector, pnls in sorted(sector_stats.items(), key=lambda x: sum(x[1])/len(x[1]), reverse=True):
        avg = sum(pnls) / len(pnls)
        wins = len([p for p in pnls if p > 0])
        print(f"    {sector:25s}: {len(pnls):>3} trades, avg {avg:+.2f}%, WR {wins/len(pnls)*100:.0f}%")

    print(f"\n  [Recent Trades (30)]")
    print(f"  {'Entry':>10} {'Exit':>10} {'Tick':>6} {'Sector':>20} {'Sc':>3} {'Entry$':>9} {'Exit$':>9} {'PnL':>7} {'Days':>4} {'Peak$':>9} {'Reason':>14}")
    print(f"  {'-'*115}")
    for t in sorted(trades, key=lambda x: x["entry_date"], reverse=True)[:30]:
        sec = (t["sector"] or "?")[:20]
        peak = t.get("peak", t["entry_price"])
        print(
            f"  {t['entry_date']:>10} {t['exit_date']:>10} {t['ticker']:>6} {sec:>20} "
            f"{t['score']:>3} ${t['entry_price']:>8.2f} ${t['exit_price']:>8.2f} "
            f"{t['pnl_pct']:>+6.2f}% {t['hold_days']:>3}d ${peak:>8.2f} {t['exit_reason']:>14}"
        )
