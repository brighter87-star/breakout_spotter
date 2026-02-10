"""디버그: 각 조건별 통과 수 확인"""
import pymysql

conn = pymysql.connect(host='localhost', port=3307, user='brighter87', password='!Wjd06Gns30', database='asset_us')
cur = conn.cursor()

VOLUME_AVG_DAYS = 10
CONSOL_MIN_DAYS = 10
CONSOL_MAX_DAYS = 130
RISE_MIN_PCT = 100.0
LOOKBACK_DAYS = 252
VOLUME_RATIO_MIN = 2.0

stats = {'total': 0, 'vol_pass': 0, 'consol_found': 0, 'breakout': 0, 'first_day': 0, 'rise_100': 0, 'floor_pass': 0}

cur.execute('SELECT s.id, s.ticker FROM bs_stocks s WHERE s.is_active = 1 ORDER BY s.id')
stocks = cur.fetchall()

for i, (stock_id, ticker) in enumerate(stocks):
    if i % 1000 == 0:
        print(f"  진행: {i}/{len(stocks)}...", flush=True)

    cur.execute(
        'SELECT trade_date, open_price, high_price, low_price, close_price, volume '
        'FROM bs_daily_prices WHERE stock_id = %s ORDER BY trade_date',
        (stock_id,))
    prices = cur.fetchall()
    if len(prices) < 70:
        continue
    stats['total'] += 1

    idx = len(prices) - 1
    today = prices[idx]
    today_close, today_vol = float(today[4]), today[5]
    if today_close <= 0 or today_vol <= 0:
        continue

    # 1) 거래량
    vol_window = prices[idx - VOLUME_AVG_DAYS:idx]
    avg_vol = sum(p[5] for p in vol_window) / len(vol_window) if vol_window else 0
    if avg_vol <= 0:
        continue
    vol_ratio = today_vol / avg_vol
    if vol_ratio < VOLUME_RATIO_MIN:
        continue
    stats['vol_pass'] += 1

    # 2) 횡보 구간
    consol_high = float(prices[idx - 1][2])
    consol_close_low = float(prices[idx - 1][4])
    best_consol = None
    for length in range(2, min(CONSOL_MAX_DAYS + 1, idx)):
        day_idx = idx - length
        consol_high = max(consol_high, float(prices[day_idx][2]))
        consol_close_low = min(consol_close_low, float(prices[day_idx][4]))
        if length >= CONSOL_MIN_DAYS:
            best_consol = {"days": length, "high": consol_high, "close_low": consol_close_low}

    if not best_consol:
        continue
    stats['consol_found'] += 1

    # 3) 신고가 돌파
    if today_close <= best_consol["high"]:
        continue
    stats['breakout'] += 1

    # 4) 첫날 체크
    if idx >= 2:
        yesterday_close = float(prices[idx - 1][4])
        prev_high = float(prices[idx - 2][2])
        for k in range(2, min(best_consol["days"] + 1, idx)):
            prev_high = max(prev_high, float(prices[idx - 1 - k][2]))
        if yesterday_close > prev_high:
            continue
    stats['first_day'] += 1

    # 5) 1년 저점 + 100% 상승
    cs = idx - best_consol["days"]
    search_start = max(0, cs - LOOKBACK_DAYS)
    if search_start >= cs:
        continue
    low = min(float(p[3]) for p in prices[search_start:cs])
    if low <= 0:
        continue
    rise = (best_consol["high"] / low - 1) * 100
    if rise < RISE_MIN_PCT:
        continue
    stats['rise_100'] += 1

    # 6) 상승분 50% 반납
    floor_price = low + (best_consol["high"] - low) * 0.5
    if best_consol["close_low"] < floor_price:
        continue
    stats['floor_pass'] += 1
    print(f"  PASS: {ticker} vol={vol_ratio:.1f}x rise={rise:.0f}% "
          f"consol={best_consol['days']}d floor=${floor_price:.2f} closeLow=${best_consol['close_low']:.2f}")

print()
print("=== 조건별 통과 수 ===")
for k, v in stats.items():
    print(f"  {k}: {v}")
conn.close()
