"""
theme_analyzer SQLite DB에서 테마-종목 매핑을 가져와 MySQL에 동기화.
"""

import sqlite3
import pymysql
from config.settings import Settings

EXCHANGE_TO_API_CODE = {
    "NASDAQ": "NAS",
    "NYSE": "NYS",
    "AMEX": "AMS",
}


def load_us_themes_from_sqlite(db_path, lookback_days=30):
    """theme_analyzer SQLite에서 US 테마-종목 매핑 조회"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """SELECT DISTINCT
               s.id as source_stock_id, s.ticker, s.name_ko, s.name_en,
               s.market, s.exchange,
               t.id as source_theme_id, t.name_ko as theme_name_ko,
               t.name_en as theme_name_en,
               dst.report_date, dst.mention_count
           FROM daily_stock_themes dst
           JOIN stocks s ON dst.stock_id = s.id
           JOIN themes t ON dst.theme_id = t.id
           WHERE s.market = 'US'
             AND dst.report_date >= date('now', ? || ' days')
             AND t.is_active = 1
           ORDER BY dst.report_date DESC, dst.mention_count DESC""",
        (f"-{lookback_days}",),
    )

    rows = cursor.fetchall()
    conn.close()

    themes = {}
    mappings = []

    for row in rows:
        tid = row["source_theme_id"]
        if tid not in themes:
            themes[tid] = {
                "source_theme_id": tid,
                "name_ko": row["theme_name_ko"],
                "name_en": row["theme_name_en"],
            }
        mappings.append({
            "ticker": row["ticker"],
            "source_theme_id": tid,
            "report_date": row["report_date"],
            "mention_count": row["mention_count"] or 1,
        })

    return list(themes.values()), mappings


def sync_themes(conn):
    """theme_analyzer → MySQL 동기화 메인 함수"""
    settings = Settings()
    db_path = settings.THEME_DB_PATH

    print("[테마 동기화] 시작...")

    try:
        themes, mappings = load_us_themes_from_sqlite(db_path)
    except Exception as e:
        print(f"[테마 동기화] SQLite 읽기 실패: {e}")
        print(f"  경로: {db_path}")
        return

    if not themes:
        print("[테마 동기화] US 테마가 없습니다.")
        return

    cursor = conn.cursor()

    # 테마 upsert
    theme_count = 0
    for t in themes:
        cursor.execute(
            """INSERT INTO bs_themes (name_ko, name_en, source_theme_id)
               VALUES (%s, %s, %s)
               ON DUPLICATE KEY UPDATE
                   name_ko = VALUES(name_ko),
                   name_en = VALUES(name_en)""",
            (t["name_ko"], t["name_en"], t["source_theme_id"]),
        )
        theme_count += 1

    conn.commit()

    # 매핑 upsert (bs_stocks와 bs_themes를 조인하여 ID 매칭)
    mapping_count = 0
    for m in mappings:
        cursor.execute("SELECT id FROM bs_stocks WHERE ticker = %s", (m["ticker"],))
        stock_row = cursor.fetchone()
        if not stock_row:
            continue

        cursor.execute(
            "SELECT id FROM bs_themes WHERE source_theme_id = %s",
            (m["source_theme_id"],),
        )
        theme_row = cursor.fetchone()
        if not theme_row:
            continue

        stock_id = stock_row[0]
        theme_id = theme_row[0]

        cursor.execute(
            """INSERT INTO bs_stock_themes (stock_id, theme_id, report_date, mention_count)
               VALUES (%s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE mention_count = VALUES(mention_count)""",
            (stock_id, theme_id, m["report_date"], m["mention_count"]),
        )
        mapping_count += 1

    conn.commit()
    print(f"[테마 동기화] 완료: {theme_count}개 테마, {mapping_count}개 매핑")
