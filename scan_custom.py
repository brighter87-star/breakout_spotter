"""
커스텀 돌파 스캐너 v3.

매수 조건:
1. 저점 대비 최소 100%(2배) 상승한 상태
2. 상승 이후 2주~6개월 횡보:
   - 종가 기준 신고가 갱신 없음 (단 하루도)
   - 신고가 갱신 시 그 날부터 다시 카운트
   - 횡보 중 고점 대비 -50% 이상 하락 없음
3. 횡보 후 10일 평균 거래량 200% 이상 동반 신고가 돌파
4. 돌파 첫날 종가 매수

성능 최적화: 전체 가격 메모리 일괄 로드
"""

import pymysql
from collections import defaultdict
import time

DB_CONFIG = {
    "host": "localhost",
    "port": 3307,
    "user": "brighter87",
    "password": "!Wjd06Gns30",
    "database": "asset_us",
}

# ── 파라미터 ──
RISE_MIN_PCT = 100.0            # 저점 대비 최소 상승률
CONSOL_MIN_DAYS = 10            # 횡보 최소 기간 (거래일) ≈ 2주
CONSOL_MAX_DAYS = 130           # 횡보 최대 기간 (거래일) ≈ 6개월
CONSOL_MAX_DROP_PCT = 50.0      # 횡보 중 고점 대비 최대 하락률
VOLUME_RATIO_MIN = 2.0          # 거래량 배수 (10일 평균 대비)
VOLUME_AVG_DAYS = 10            # 평균 거래량 기준 일수
CHECK_LAST_N_DAYS = 5           # 최근 N거래일 내 돌파 검색


def check_breakout(prices, today_idx):
    """
    돌파 조건 확인.

    핵심 로직 (횡보 판정):
    - today 직전부터 역방향으로 탐색
    - 종가 기준 최고점(peak)을 찾음
    - peak 이후 today까지 종가가 peak를 한 번도 넘지 않아야 함
    - peak~today 사이 기간이 10~130거래일
    - 그 사이 종가가 peak * 0.5 아래로 내려가면 안 됨
    """
    today = prices[today_idx]
    today_date = today[0]
    today_close = float(today[4])
    today_vol = today[5]

    if today_close <= 0 or today_vol <= 0:
        return None

    # ── 1. 거래량 체크 (가장 빠른 필터) ──
    vol_window = prices[today_idx - VOLUME_AVG_DAYS:today_idx]
    if len(vol_window) < VOLUME_AVG_DAYS:
        return None

    avg_vol = sum(p[5] for p in vol_window) / len(vol_window)
    if avg_vol <= 0:
        return None

    volume_ratio = today_vol / avg_vol
    if volume_ratio < VOLUME_RATIO_MIN:
        return None

    # ── 2. 횡보 구간의 고점(peak) 찾기 ──
    # today 직전부터 역방향으로, 종가 기준 최고점을 찾는다.
    # 그 최고점이 "횡보의 시작점"이 된다.
    peak_close = 0.0
    peak_idx = -1

    search_limit = min(CONSOL_MAX_DAYS, today_idx)
    for offset in range(1, search_limit + 1):
        idx = today_idx - offset
        day_close = float(prices[idx][4])
        if day_close > peak_close:
            peak_close = day_close
            peak_idx = idx

    if peak_idx < 0 or peak_close <= 0:
        return None

    # ── 3. 신고가 돌파 확인: 오늘 종가 > 횡보 고점 ──
    if today_close <= peak_close:
        return None

    # ── 4. 횡보 기간 확인 (peak → today) ──
    consol_days = today_idx - peak_idx
    if consol_days < CONSOL_MIN_DAYS or consol_days > CONSOL_MAX_DAYS:
        return None

    # ── 5. 횡보 중 -50% 하락 금지 (종가 기준) ──
    floor_price = peak_close * (1 - CONSOL_MAX_DROP_PCT / 100)
    consol_close_low = peak_close
    for k in range(peak_idx + 1, today_idx):
        c = float(prices[k][4])
        if c < consol_close_low:
            consol_close_low = c
    if consol_close_low < floor_price:
        return None

    # ── 6. 첫날 돌파 확인: 전날 종가는 peak 이하여야 함 ──
    if today_idx >= 1:
        yesterday_close = float(prices[today_idx - 1][4])
        if yesterday_close > peak_close:
            return None

    # ── 7. 저점 대비 100% 상승 확인 ──
    # peak 이전 데이터에서 저점 탐색 (횡보 시작 전 상승 구간)
    if peak_idx < 10:
        return None

    # peak 이전 전체에서 저점 찾기
    low_price = float('inf')
    for k in range(0, peak_idx):
        lo = float(prices[k][3])  # low_price
        if lo < low_price:
            low_price = lo

    if low_price <= 0:
        return None

    rise_pct = (peak_close / low_price - 1) * 100
    if rise_pct < RISE_MIN_PCT:
        return None

    consol_range_pct = ((peak_close / consol_close_low) - 1) * 100

    return {
        "close": today_close,
        "low": low_price,
        "peak": peak_close,
        "rise_pct": rise_pct,
        "floor": floor_price,
        "consol_close_low": consol_close_low,
        "consol_days": consol_days,
        "consol_range_pct": consol_range_pct,
        "volume_ratio": volume_ratio,
    }


def scan():
    conn = pymysql.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("SELECT MAX(trade_date) FROM bs_daily_prices")
    latest_date = cur.fetchone()[0]

    print(f"DB 최신 거래일: {latest_date}")
    print(f"스캔 조건:")
    print(f"  1. 저점 대비 {RISE_MIN_PCT}%+ 상승")
    print(f"  2. 종가 신고가 후 {CONSOL_MIN_DAYS}~{CONSOL_MAX_DAYS}거래일 횡보 (신고가 갱신 없음)")
    print(f"  3. 횡보 중 고점 대비 -{CONSOL_MAX_DROP_PCT}% 이상 하락 없음")
    print(f"  4. 거래량 {VOLUME_RATIO_MIN}x+ 동반 돌파 (첫날만)")
    print()

    # ── 전체 가격 메모리 로드 (성능 최적화) ──
    t0 = time.time()
    print("가격 데이터 로딩 중...", flush=True)

    cur.execute("""
        SELECT s.id, s.ticker, s.name, s.market_cap
        FROM bs_stocks s
        WHERE s.is_active = 1
        ORDER BY s.id
    """)
    stocks = {row[0]: {"ticker": row[1], "name": row[2], "market_cap": row[3]} for row in cur.fetchall()}

    cur.execute("""
        SELECT stock_id, trade_date, open_price, high_price, low_price, close_price, volume
        FROM bs_daily_prices
        WHERE stock_id IN (SELECT id FROM bs_stocks WHERE is_active = 1)
        ORDER BY stock_id, trade_date
    """)

    all_prices = defaultdict(list)
    for row in cur:
        stock_id = row[0]
        all_prices[stock_id].append(row[1:])  # (date, open, high, low, close, volume)

    t1 = time.time()
    print(f"로딩 완료: {len(all_prices)}개 종목, {sum(len(v) for v in all_prices.values()):,}개 캔들 ({t1-t0:.1f}초)")
    print()

    # ── 스캔 ──
    signals = []
    total = len(all_prices)

    for i, (stock_id, prices) in enumerate(all_prices.items()):
        if i % 1000 == 0:
            print(f"  스캔: {i}/{total}...", flush=True)

        if len(prices) < CONSOL_MIN_DAYS + VOLUME_AVG_DAYS + 20:
            continue

        info = stocks.get(stock_id)
        if not info:
            continue

        # 최근 N거래일 내 돌파 확인
        for day_offset in range(CHECK_LAST_N_DAYS):
            idx = len(prices) - 1 - day_offset
            if idx < CONSOL_MAX_DAYS + 10:
                break
            result = check_breakout(prices, idx)
            if result:
                result["ticker"] = info["ticker"]
                result["name"] = info["name"]
                result["market_cap"] = info["market_cap"]
                result["signal_date"] = str(prices[idx][0])
                signals.append(result)
                break

    t2 = time.time()
    print(f"\n{'='*90}")
    print(f"스캔 완료! 조건 충족: {len(signals)}개  (기준일: {latest_date}, 소요: {t2-t0:.1f}초)")
    print(f"{'='*90}\n")

    if not signals:
        print("조건을 충족하는 종목이 없습니다.")
        conn.close()
        return

    signals.sort(key=lambda x: x["volume_ratio"], reverse=True)

    print(f"{'#':>3}  {'티커':<7} {'시그널일':>10} {'종가':>9} {'저점':>8} {'고점':>8} "
          f"{'상승률':>7} {'횡보일':>5} {'횡보폭':>6} {'거래량비':>7} {'시총($B)':>8}  이름")
    print("-" * 125)

    for rank, s in enumerate(signals, 1):
        mcap_str = f"{s['market_cap']/1e9:.1f}" if s['market_cap'] else "N/A"
        print(
            f"{rank:>3}  {s['ticker']:<7} "
            f"{s['signal_date']} "
            f"${s['close']:>7.2f} "
            f"${s['low']:>6.2f} "
            f"${s['peak']:>6.2f} "
            f"{s['rise_pct']:>6.1f}% "
            f"{s['consol_days']:>4}일 "
            f"{s['consol_range_pct']:>5.1f}% "
            f"{s['volume_ratio']:>6.1f}x "
            f"{mcap_str:>8} "
            f" {(s['name'] or '')[:25]}"
        )

    conn.close()


if __name__ == "__main__":
    scan()
