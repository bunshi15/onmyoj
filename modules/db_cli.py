# modules/db_cli.py

import typer
import sqlite3
import pandas as pd

app = typer.Typer()
DB_PATH = "onmyoj.db"

def get_conn():
    return sqlite3.connect(DB_PATH)

def set_current_session(session_id):
    with open('.current_session', 'w') as f:
        f.write(str(session_id))

def get_current_session():
    try:
        with open('.current_session', 'r') as f:
            return int(f.read().strip())
    except Exception:
        return None

@app.command()
def list_sessions():
    """Показать все сессии"""
    conn = get_conn()
    df = pd.read_sql_query("SELECT session_id, started_at, keyword, comment FROM sessions ORDER BY started_at DESC", conn)
    print(df)
    conn.close()

@app.command()
def use_session(session_id: int):
    """Сделать сессию активной для просмотра"""
    set_current_session(session_id)
    print(f"Сессия {session_id} выбрана как текущая")

@app.command()
def current_session():
    """Показать текущую выбранную сессию"""
    session_id = get_current_session()
    if not session_id:
        print("Текущая сессия не выбрана.")
        return
    conn = get_conn()
    df = pd.read_sql_query(
        "SELECT session_id, started_at, keyword, comment FROM sessions WHERE session_id = ?",
        conn, params=(session_id,))
    print(df)
    conn.close()

@app.command()
def stats(session_id: int = None):
    """Краткая статистика по выбранной сессии"""
    session_id = session_id or get_current_session()
    if not session_id:
        print("Нет активной сессии. Укажите --session-id или выберите use_session.")
        return
    conn = get_conn()
    c = conn.cursor()
    print(f"=== СТАТИСТИКА по сессии {session_id} ===")
    for name, table in [
        ("Всего видео", "videos"),
        ("Контактов из описаний", "video_contacts"),
        ("Каналов", "channels"),
        ("Контактов из каналов", "channel_contacts"),
        ("Комментариев", "comments"),
        ("Контактов из комментариев", "comment_contacts")
    ]:
        try:
            c.execute(f"SELECT COUNT(*) FROM {table} WHERE session_id = ?", (session_id,))
            print(f"{name}: {c.fetchone()[0]}")
        except Exception:
            print(f"{name}: -")
    conn.close()

@app.command()
def show_videos(limit: int = 10, session_id: int = None):
    """Список видео по выбранной сессии"""
    session_id = session_id or get_current_session()
    if not session_id:
        print("Нет активной сессии. Укажите --session-id или выберите use_session.")
        return
    conn = get_conn()
    df = pd.read_sql_query(
        "SELECT video_id, title, channel_name, published_time, view_count FROM videos WHERE session_id = ? LIMIT ?",
        conn, params=(session_id, limit))
    print(df)
    conn.close()

@app.command()
def show_channels(limit: int = 10, min_subs: int = 0, session_id: int = None):
    """Список каналов (top-N, фильтр по подписчикам, только по выбранной сессии)"""
    session_id = session_id or get_current_session()
    if not session_id:
        print("Нет активной сессии. Укажите --session-id или выберите use_session.")
        return
    conn = get_conn()
    df = pd.read_sql_query("""
                           SELECT channel_id, title, subscriber_count, video_count
                           FROM channels
                           WHERE CAST(subscriber_count AS INTEGER) >= ?
                             AND session_id = ?
                           ORDER BY CAST(subscriber_count AS INTEGER) DESC
                               LIMIT ?
                           """, conn, params=(min_subs, session_id, limit))
    print(df)
    conn.close()


@app.command()
def search_contacts(
        contact_type: str = None,
        search_term: str = None,
        source: str = "all",  # 'all', 'video', 'comment', 'channel'
        limit: int = 20,
        session_id: int = None
):
    """
    Поиск контактов по всем таблицам (описания, комментарии, каналы).
    Опционально: фильтр по типу, по значению, по источнику.
    """
    session_id = session_id or get_current_session()
    if not session_id:
        print("Нет активной сессии. Укажите --session-id или выберите use_session.")
        return
    conn = get_conn()
    queries = []
    if source in ("all", "video"):
        queries.append((
            "video",
            """
            SELECT 'video' as source, v.title, v.url, vc.contact_type, vc.value
            FROM video_contacts vc
                     JOIN videos v ON v.video_id = vc.video_id
            WHERE v.session_id = ?
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
            WHERE cc.session_id = ?
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
            WHERE cc.session_id = ?
            """,
            "cc"
        ))

    for _, q, alias in queries:
        params = [session_id]
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
def analyze_channels(session_id: int = None):
    """Агрегация по каналам в рамках выбранной сессии"""
    session_id = session_id or get_current_session()
    if not session_id:
        print("Нет активной сессии.")
        return
    conn = get_conn()
    c = conn.cursor()

    print("КАНАЛЫ С КОНТАКТАМИ:")
    c.execute("""
              SELECT c.title, c.subscriber_count, COUNT(cc.id) as contact_count
              FROM channels c
                       LEFT JOIN channel_contacts cc ON c.channel_id = cc.channel_id
              WHERE c.session_id = ?
              GROUP BY c.channel_id
              HAVING contact_count > 0
              ORDER BY CAST(c.subscriber_count AS INTEGER) DESC
              """, (session_id,))
    for row in c.fetchall():
        print(f"{row[0]} — Подписчиков: {row[1]}, Контактов: {row[2]}")

    print("\nКРУПНЫЕ КАНАЛЫ БЕЗ КОНТАКТОВ (10k+ подписчиков):")
    c.execute("""
              SELECT c.title, c.subscriber_count, c.video_count
              FROM channels c
                       LEFT JOIN channel_contacts cc ON c.channel_id = cc.channel_id
              WHERE cc.id IS NULL
                AND c.session_id = ?
                AND CAST(c.subscriber_count AS INTEGER) > 10000
              ORDER BY CAST(c.subscriber_count AS INTEGER) DESC
                  LIMIT 10
              """, (session_id,))
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
