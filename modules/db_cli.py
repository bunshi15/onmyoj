# modules/db_cli.py

import typer
import sqlite3
import pandas as pd

app = typer.Typer()
DB_PATH = "onmyoj.db"

def get_conn():
    return sqlite3.connect(DB_PATH)

@app.command()
def stats():
    """Краткая статистика по базе"""
    conn = get_conn()
    c = conn.cursor()
    print("=== СТАТИСТИКА ===")
    for name, table in [
        ("Всего видео", "videos"),
        ("Контактов из описаний", "video_contacts"),
        ("Каналов", "channels"),
        ("Контактов из каналов", "channel_contacts"),
        ("Комментариев", "comments"),
        ("Контактов из комментариев", "comment_contacts")
    ]:
        c.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"{name}: {c.fetchone()[0]}")
    conn.close()

@app.command()
def show_videos(limit: int = 10):
    """Список видео с базовой инфой"""
    conn = get_conn()
    df = pd.read_sql_query("SELECT video_id, title, channel_name, published_time, view_count FROM videos LIMIT ?", conn, params=(limit,))
    print(df)
    conn.close()

@app.command()
def show_channels(limit: int = 10, min_subs: int = 0):
    """Список каналов (top-N, фильтр по подписчикам)"""
    conn = get_conn()
    df = pd.read_sql_query("""
                           SELECT channel_id, title, subscriber_count, video_count
                           FROM channels
                           WHERE CAST(subscriber_count AS INTEGER) >= ?
                           ORDER BY CAST(subscriber_count AS INTEGER) DESC
                               LIMIT ?
                           """, conn, params=(min_subs, limit))
    print(df)
    conn.close()

@app.command()
def search_contacts(
        contact_type: str = None,
        search_term: str = None,
        source: str = "all",  # 'all', 'video', 'comment', 'channel'
        limit: int = 20
):
    """
    Поиск контактов по всем таблицам (описания, комментарии, каналы).
    Опционально: фильтр по типу, по значению, по источнику.
    """
    conn = get_conn()
    queries = []
    if source in ("all", "video"):
        queries.append((
            "video",
            """
            SELECT 'video' as source, v.title, v.url, vc.contact_type, vc.value
            FROM video_contacts vc
                     JOIN videos v ON v.video_id = vc.video_id
            WHERE 1=1
            """,
            "vc"
        ))
    if source in ("all", "comment"):
        queries.append((
            "comment",
            """
            SELECT 'comment' as source, v.title, v.url, cc.contact_type, cc.value
            FROM comment_contacts cc
                     JOIN comments c ON c.id = cc.comment_id
                     JOIN videos v ON v.video_id = c.video_id
            WHERE 1=1
            """,
            "cc"
        ))
    if source in ("all", "channel"):
        queries.append((
            "channel",
            """
            SELECT 'channel' as source, c.title, NULL as url, cc.contact_type, cc.value
            FROM channel_contacts cc
                     JOIN channels c ON c.channel_id = cc.channel_id
            WHERE 1=1
            """,
            "cc"
        ))

    for _, q, alias in queries:
        params = []
        if contact_type:
            q += f" AND {alias}.contact_type = ?"
            params.append(contact_type)
        if search_term:
            q += f" AND {alias}.value LIKE ?"
            params.append(f"%{search_term}%")
        q += " LIMIT ?"
        params.append(limit)
        df = pd.read_sql_query(q, conn, params=tuple(params))
        if not df.empty:
            print(df)
    conn.close()

@app.command()
def analyze_channels():
    """Агрегация по каналам: топы, контакты, крупные каналы без контактов"""
    conn = get_conn()
    c = conn.cursor()

    print("КАНАЛЫ С КОНТАКТАМИ:")
    c.execute("""
              SELECT c.title, c.subscriber_count, COUNT(cc.id) as contact_count
              FROM channels c
                       LEFT JOIN channel_contacts cc ON c.channel_id = cc.channel_id
              GROUP BY c.channel_id
              HAVING contact_count > 0
              ORDER BY CAST(c.subscriber_count AS INTEGER) DESC
              """)
    for row in c.fetchall():
        print(f"{row[0]} — Подписчиков: {row[1]}, Контактов: {row[2]}")

    print("\nКРУПНЫЕ КАНАЛЫ БЕЗ КОНТАКТОВ (10k+ подписчиков):")
    c.execute("""
              SELECT c.title, c.subscriber_count, c.video_count
              FROM channels c
                       LEFT JOIN channel_contacts cc ON c.channel_id = cc.channel_id
              WHERE cc.id IS NULL
                AND CAST(c.subscriber_count AS INTEGER) > 10000
              ORDER BY CAST(c.subscriber_count AS INTEGER) DESC
                  LIMIT 10
              """)
    for row in c.fetchall():
        print(f"{row[0]}: {row[1]} подписчиков, {row[2]} видео")

    conn.close()

@app.command()
def export_report(fmt: str = "html"):
    """Экспорт отчёта по видео (html/csv)"""
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM videos", conn)
    if fmt == "html":
        df.to_html("report.html", index=False)
        print("Сохранено в report.html")
    elif fmt == "csv":
        df.to_csv("report.csv", index=False)
        print("Сохранено в report.csv")
    conn.close()

if __name__ == "__main__":
    app()
