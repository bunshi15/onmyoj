# view_data.py
import sqlite3
import sys

def view_data(db_path="onmyoj.db"):
    """Просмотр собранных данных"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Статистика
    print("=== СТАТИСТИКА ===")
    cursor.execute("SELECT COUNT(*) FROM videos")
    video_count = cursor.fetchone()[0]
    print(f"Всего видео: {video_count}")

    cursor.execute("SELECT COUNT(*) FROM video_contacts")
    contact_count = cursor.fetchone()[0]
    print(f"Контактов из описаний: {contact_count}")

    cursor.execute("SELECT COUNT(*) FROM channels")
    channel_count = cursor.fetchone()[0]
    print(f"Каналов: {channel_count}")

    cursor.execute("SELECT COUNT(*) FROM channel_contacts")
    channel_contact_count = cursor.fetchone()[0]
    print(f"Контактов из каналов: {channel_contact_count}")

    cursor.execute("SELECT COUNT(*) FROM comments")
    comment_count = cursor.fetchone()[0]
    print(f"Комментариев: {comment_count}")

    cursor.execute("SELECT COUNT(*) FROM comment_contacts")
    comment_contact_count = cursor.fetchone()[0]
    print(f"Контактов из комментариев: {comment_contact_count}")

    # Топ контактов
    print("\n=== ТОП ТИПОВ КОНТАКТОВ ===")
    cursor.execute("""
                   SELECT contact_type, COUNT(*) as cnt
                   FROM video_contacts
                   GROUP BY contact_type
                   ORDER BY cnt DESC
                   """)
    for row in cursor.fetchall():
        print(f"{row[0]}: {row[1]}")

    # Топ каналов по подписчикам
    print("\n=== ТОП КАНАЛОВ ПО ПОДПИСЧИКАМ ===")
    cursor.execute("""
                   SELECT title, subscriber_count, video_count
                   FROM channels
                   WHERE CAST(subscriber_count AS INTEGER) > 0
                   ORDER BY CAST(subscriber_count AS INTEGER) DESC
                       LIMIT 5
                   """)
    for row in cursor.fetchall():
        print(f"{row[0]}: {row[1]} подписчиков, {row[2]} видео")

    # Примеры найденных контактов
    print("\n=== ПРИМЕРЫ КОНТАКТОВ ===")
    cursor.execute("""
                   SELECT v.title, vc.contact_type, vc.value
                   FROM video_contacts vc
                            JOIN videos v ON v.video_id = vc.video_id
                       LIMIT 10
                   """)
    for row in cursor.fetchall():
        print(f"Видео: {row[0][:50]}...")
        print(f"  {row[1]}: {row[2]}")
        print()

    conn.close()

def search_contacts(contact_type=None, search_term=None, db_path="onmyoj.db"):
    """Поиск контактов"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
            SELECT v.title, v.url, vc.contact_type, vc.value
            FROM video_contacts vc
                     JOIN videos v ON v.video_id = vc.video_id
            WHERE 1=1 \
            """
    params = []

    if contact_type:
        query += " AND vc.contact_type = ?"
        params.append(contact_type)

    if search_term:
        query += " AND vc.value LIKE ?"
        params.append(f"%{search_term}%")

    cursor.execute(query, params)

    print(f"\n=== РЕЗУЛЬТАТЫ ПОИСКА ===")
    for row in cursor.fetchall():
        print(f"Видео: {row[0][:50]}...")
        print(f"URL: {row[1]}")
        print(f"{row[2]}: {row[3]}")
        print("-" * 50)

    conn.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "search":
            contact_type = sys.argv[2] if len(sys.argv) > 2 else None
            search_term = sys.argv[3] if len(sys.argv) > 3 else None
            search_contacts(contact_type, search_term)
        else:
            print("Использование: python view_data.py [search [тип_контакта] [поисковый_термин]]")
    else:
        view_data()