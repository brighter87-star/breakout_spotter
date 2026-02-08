"""
돌파 패턴 탐지 + 테마 강도 분석.

패턴 조건 (모두 충족):
A. 3개월(60거래일) 신고가 돌파
B. 바닥 대비 50~100% 상승 상태
C. 2주 이상(10거래일+) 기간 조정 (가격 범위 ≤ 10%)
D. 조정 구간 고가 재돌파
"""

import pymysql
from config.settings import Settings


# ── 테마 강도 ────────────────────────────────────────────

def _get_spy_id(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM bs_stocks WHERE ticker = 'SPY'")
    row = cursor.fetchone()
    return row[0] if row else None


def _calc_return(conn, stock_id, period_days):
    """stock_id의 period_days 거래일 수익률(%) 계산"""
    cursor = conn.cursor()
    cursor.execute(
        """SELECT close_price FROM bs_daily_prices
           WHERE stock_id = %s ORDER BY trade_date DESC LIMIT %s""",
        (stock_id, period_days + 1),
    )
    rows = cursor.fetchall()
    if len(rows) < period_days + 1:
        return None
    current = float(rows[0][0])
    past = float(rows[period_days][0])
    if past <= 0:
        return None
    return ((current / past) - 1) * 100


def calc_theme_strength(conn, period_days=20):
    """모든 테마의 강도를 계산하여 딕셔너리로 반환"""
    spy_id = _get_spy_id(conn)
    spy_return = _calc_return(conn, spy_id, period_days) if spy_id else 0.0
    if spy_return is None:
        spy_return = 0.0

    cursor = conn.cursor(pymysql.cursors.DictCursor)

    # 가장 최근 report_date 기준 테마별 종목 조회
    cursor.execute(
        """SELECT st.theme_id, st.stock_id, t.name_ko
           FROM bs_stock_themes st
           JOIN bs_themes t ON st.theme_id = t.id
           WHERE st.report_date = (
               SELECT MAX(report_date) FROM bs_stock_themes WHERE theme_id = st.theme_id
           )"""
    )
    rows = cursor.fetchall()

    # 테마별 그룹핑
    theme_stocks = {}
    theme_names = {}
    for row in rows:
        tid = row["theme_id"]
        if tid not in theme_stocks:
            theme_stocks[tid] = []
            theme_names[tid] = row["name_ko"]
        theme_stocks[tid].append(row["stock_id"])

    results = {}
    for tid, stock_ids in theme_stocks.items():
        returns = []
        for sid in stock_ids:
            r = _calc_return(conn, sid, period_days)
            if r is not None:
                returns.append(r)

        if not returns:
            continue

        avg_ret = sum(returns) / len(returns)
        rel_str = avg_ret - spy_return

        results[tid] = {
            "theme_id": tid,
            "name_ko": theme_names[tid],
            "avg_return_pct": round(avg_ret, 4),
            "spy_return_pct": round(spy_return, 4),
            "relative_str": round(rel_str, 4),
            "stock_count": len(returns),
        }

    return results


# ── 돌파 패턴 탐지 ──────────────────────────────────────

def check_breakout(prices, settings):
    """
    가격 리스트(과거→최신)에서 돌파 패턴 검사.

    Args:
        prices: list of (trade_date, open, high, low, close, volume)
        settings: Settings 객체

    Returns:
        dict with breakout metrics if triggered, None otherwise
    """
    n = len(prices)
    if n < settings.HIGH_BREAKOUT_DAYS + 10:
        return None

    today = prices[-1]
    today_date, today_open, today_high, today_low, today_close, today_vol = today

    # A. 60거래일 신고가 돌파
    lookback_start = max(0, n - settings.HIGH_BREAKOUT_DAYS - 1)
    high_60d = max(p[2] for p in prices[lookback_start:n - 1])  # 오늘 제외 직전 60일
    if today_high <= high_60d:
        return None

    # B. 바닥 대비 50~100% 상승
    low_all = min(p[3] for p in prices)
    if low_all <= 0:
        return None
    rise_pct = ((today_close / low_all) - 1) * 100
    if rise_pct < settings.RISE_FROM_LOW_MIN_PCT or rise_pct > settings.RISE_FROM_LOW_MAX_PCT:
        return None

    # C. 기간 조정 탐지 (오늘 직전부터 역방향 탐색)
    consolidation = _find_consolidation(prices, settings)
    if consolidation is None:
        return None

    # D. 조정 구간 고가 재돌파
    if today_close <= consolidation["high"]:
        return None

    # E. 거래량 조건: 돌파일 거래량 ≥ 직전 10일 평균의 1.5배
    vol_days = settings.VOLUME_RATIO_DAYS
    vol_window = prices[max(0, n - vol_days - 1):n - 1]
    avg_vol = sum(p[5] for p in vol_window) / len(vol_window) if vol_window else 1
    volume_ratio = today_vol / avg_vol if avg_vol > 0 else 0
    if volume_ratio < settings.VOLUME_RATIO_MIN:
        return None

    return {
        "signal_date": today_date,
        "close_price": today_close,
        "high_60d": high_60d,
        "low_lookback": low_all,
        "rise_from_low_pct": round(rise_pct, 4),
        "consolidation_days": consolidation["days"],
        "consolidation_range_pct": consolidation["range_pct"],
        "consolidation_high": consolidation["high"],
        "volume_ratio": round(volume_ratio, 4),
    }


def _find_consolidation(prices, settings):
    """
    오늘 직전에서 역방향으로 조정 구간 탐색.
    10거래일 이상, 가격 범위 ≤ 10%.
    """
    n = len(prices)
    cons_high = 0
    cons_low = float("inf")
    cons_days = 0

    for i in range(n - 2, max(0, n - 62), -1):
        test_high = max(cons_high, prices[i][2])
        test_low = min(cons_low, prices[i][3])
        if test_low <= 0:
            break
        test_mid = (test_high + test_low) / 2
        range_pct = ((test_high - test_low) / test_mid) * 100

        if range_pct <= settings.CONSOLIDATION_MAX_RANGE_PCT:
            cons_days += 1
            cons_high = test_high
            cons_low = test_low
        else:
            break

    if cons_days >= settings.CONSOLIDATION_MIN_DAYS:
        mid = (cons_high + cons_low) / 2
        range_pct = ((cons_high - cons_low) / mid) * 100
        return {
            "days": cons_days,
            "high": cons_high,
            "low": cons_low,
            "range_pct": round(range_pct, 4),
        }
    return None


def score_breakout(breakout, theme_rel_str=None):
    """신호 점수 (0~100) 계산"""
    vr = breakout.get("volume_ratio", 0)
    if vr >= 3.0:
        vol_score = 25
    elif vr >= 2.0:
        vol_score = 20
    elif vr >= 1.5:
        vol_score = 15
    elif vr >= 1.0:
        vol_score = 10
    else:
        vol_score = 5

    if theme_rel_str is not None:
        if theme_rel_str >= 10.0:
            theme_score = 25
        elif theme_rel_str >= 5.0:
            theme_score = 20
        elif theme_rel_str >= 2.0:
            theme_score = 15
        elif theme_rel_str >= 0.0:
            theme_score = 10
        else:
            theme_score = 0
    else:
        theme_score = 12  # 테마 정보 없으면 중간값

    rise = breakout.get("rise_from_low_pct", 0)
    rise_diff = abs(rise - 70)
    if rise_diff <= 10:
        rise_score = 25
    elif rise_diff <= 20:
        rise_score = 20
    elif rise_diff <= 30:
        rise_score = 15
    else:
        rise_score = 10

    cr = breakout.get("consolidation_range_pct", 100)
    cd = breakout.get("consolidation_days", 0)
    if cr <= 5 and cd >= 15:
        cons_score = 25
    elif cr <= 7 and cd >= 12:
        cons_score = 20
    elif cr <= 10 and cd >= 10:
        cons_score = 15
    else:
        cons_score = 10

    return vol_score + theme_score + rise_score + cons_score


# ── 스캔 메인 ───────────────────────────────────────────

def scan_breakouts(conn):
    """전 종목 돌파 패턴 스캔"""
    settings = Settings()
    cursor = conn.cursor(pymysql.cursors.DictCursor)

    print("[돌파 스캔] 시작...")

    # 테마 강도 계산
    print("[돌파 스캔] 테마 강도 계산 중...")
    theme_strength = calc_theme_strength(conn)
    for tid, ts in theme_strength.items():
        rel = ts["relative_str"]
        marker = "+" if rel > 0 else ""
        print(f"  {ts['name_ko']}: {marker}{rel:.1f}% (SPY 대비), {ts['stock_count']}종목")

    # 종목별 테마 매핑 (가장 강한 테마 기준)
    cursor.execute(
        """SELECT st.stock_id, st.theme_id
           FROM bs_stock_themes st
           WHERE st.report_date = (
               SELECT MAX(report_date) FROM bs_stock_themes WHERE theme_id = st.theme_id
           )"""
    )
    stock_theme_map = {}
    for row in cursor.fetchall():
        sid = row["stock_id"]
        tid = row["theme_id"]
        if sid not in stock_theme_map:
            stock_theme_map[sid] = tid
        elif tid in theme_strength and (
            stock_theme_map[sid] not in theme_strength
            or theme_strength[tid]["relative_str"] > theme_strength[stock_theme_map[sid]]["relative_str"]
        ):
            stock_theme_map[sid] = tid

    # 전 종목 스캔
    cursor.execute(
        "SELECT id, ticker, exchange_code FROM bs_stocks WHERE is_active = 1"
    )
    stocks = cursor.fetchall()

    signals = []
    scanned = 0

    for s in stocks:
        stock_id = s["id"]

        # 가격 데이터 조회 (과거→최신)
        cursor.execute(
            """SELECT trade_date, open_price, high_price, low_price, close_price, volume
               FROM bs_daily_prices
               WHERE stock_id = %s
               ORDER BY trade_date ASC""",
            (stock_id,),
        )
        prices = cursor.fetchall()

        if len(prices) < settings.HIGH_BREAKOUT_DAYS + 10:
            continue

        scanned += 1

        # 튜플을 리스트로 변환 (date, open, high, low, close, volume)
        price_list = [
            (str(p["trade_date"]), float(p["open_price"]), float(p["high_price"]),
             float(p["low_price"]), float(p["close_price"]), int(p["volume"]))
            for p in prices
        ]

        breakout = check_breakout(price_list, settings)
        if breakout is None:
            continue

        # 테마 정보
        theme_id = stock_theme_map.get(stock_id)
        theme_rel = None
        if theme_id and theme_id in theme_strength:
            theme_rel = theme_strength[theme_id]["relative_str"]

        score = score_breakout(breakout, theme_rel)

        signal = {
            "stock_id": stock_id,
            "ticker": s["ticker"],
            **breakout,
            "theme_id": theme_id,
            "theme_relative_str": theme_rel,
            "signal_score": score,
        }
        signals.append(signal)

    # 점수 순 정렬
    signals.sort(key=lambda x: x["signal_score"], reverse=True)

    # DB 저장
    insert_cursor = conn.cursor()
    for sig in signals:
        try:
            insert_cursor.execute(
                """INSERT INTO bs_breakout_signals
                   (stock_id, signal_date, theme_id, close_price, high_60d,
                    low_lookback, rise_from_low_pct, consolidation_days,
                    consolidation_range_pct, consolidation_high, volume_ratio,
                    theme_relative_str, signal_score)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                       theme_id = VALUES(theme_id),
                       close_price = VALUES(close_price),
                       high_60d = VALUES(high_60d),
                       low_lookback = VALUES(low_lookback),
                       rise_from_low_pct = VALUES(rise_from_low_pct),
                       consolidation_days = VALUES(consolidation_days),
                       consolidation_range_pct = VALUES(consolidation_range_pct),
                       consolidation_high = VALUES(consolidation_high),
                       volume_ratio = VALUES(volume_ratio),
                       theme_relative_str = VALUES(theme_relative_str),
                       signal_score = VALUES(signal_score)""",
                (
                    sig["stock_id"], sig["signal_date"], sig.get("theme_id"),
                    sig["close_price"], sig["high_60d"], sig["low_lookback"],
                    sig["rise_from_low_pct"], sig["consolidation_days"],
                    sig["consolidation_range_pct"], sig["consolidation_high"],
                    sig["volume_ratio"], sig.get("theme_relative_str"),
                    sig["signal_score"],
                ),
            )
        except pymysql.Error as e:
            print(f"  [WARN] {sig['ticker']} 저장 실패: {e}")

    conn.commit()

    # 결과 출력
    print(f"\n[돌파 스캔] 완료: {scanned}개 종목 스캔, {len(signals)}개 신호 발견")
    if signals:
        print(f"\n{'순위':>4} {'종목':>8} {'점수':>5} {'종가':>10} {'상승률':>7} {'조정일':>5} {'조정폭':>6} {'거래량비':>7} {'테마강도':>7}")
        print("-" * 80)
        for i, sig in enumerate(signals[:20]):
            theme_str = f"{sig['theme_relative_str']:+.1f}%" if sig.get("theme_relative_str") is not None else "N/A"
            print(
                f"{i + 1:>4} {sig['ticker']:>8} {sig['signal_score']:>5.0f} "
                f"${sig['close_price']:>9.2f} {sig['rise_from_low_pct']:>6.1f}% "
                f"{sig['consolidation_days']:>5}일 {sig['consolidation_range_pct']:>5.1f}% "
                f"{sig['volume_ratio']:>6.1f}x {theme_str:>7}"
            )

    return signals
