# analyze_channels.py
import sqlite3
from datetime import datetime

def analyze_channels(db_path="onmyoj.db"):
    """Анализ каналов и их контактов"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("=== АНАЛИЗ КАНАЛОВ ===\n")

    # Каналы с контактами
    print("КАНАЛЫ С КОНТАКТАМИ:")
    cursor.execute("""
                   SELECT c.title, c.subscriber_count, COUNT(cc.id) as contact_count
                   FROM channels c
                            LEFT JOIN channel_contacts cc ON c.channel_id = cc.channel_id
                   GROUP BY c.channel_id
                   HAVING contact_count > 0
                   ORDER BY CAST(c.subscriber_count AS INTEGER) DESC
                   """)

    for row in cursor.fetchall():
        print(f"\nКанал: {row[0]}")
        print(f"Подписчиков: {row[1]}")
        print(f"Найдено контактов: {row[2]}")

    # Детальная информация о контактах каналов
    print("\n\n=== КОНТАКТЫ КАНАЛОВ ===")
    cursor.execute("""
                   SELECT c.title, c.channel_id, cc.contact_type, cc.value
                   FROM channel_contacts cc
                            JOIN channels c ON c.channel_id = cc.channel_id
                   ORDER BY CAST(c.subscriber_count AS INTEGER) DESC
                   """)

    current_channel = None
    for row in cursor.fetchall():
        if current_channel != row[0]:
            current_channel = row[0]
            print(f"\n{current_channel}:")
            print(f"  https://youtube.com/channel/{row[1]}")
        print(f"  {row[2]}: {row[3]}")

    # Статистика по типам контактов в каналах
    print("\n\n=== СТАТИСТИКА КОНТАКТОВ В КАНАЛАХ ===")
    cursor.execute("""
                   SELECT contact_type, COUNT(*) as cnt
                   FROM channel_contacts
                   GROUP BY contact_type
                   ORDER BY cnt DESC
                   """)

    for row in cursor.fetchall():
        print(f"{row[0]}: {row[1]}")

    # Каналы без контактов но с большим количеством подписчиков
    print("\n\n=== КРУПНЫЕ КАНАЛЫ БЕЗ КОНТАКТОВ ===")
    cursor.execute("""
                   SELECT c.title, c.subscriber_count, c.video_count
                   FROM channels c
                            LEFT JOIN channel_contacts cc ON c.channel_id = cc.channel_id
                   WHERE cc.id IS NULL
                     AND CAST(c.subscriber_count AS INTEGER) > 10000
                   ORDER BY CAST(c.subscriber_count AS INTEGER) DESC
                       LIMIT 10
                   """)

    for row in cursor.fetchall():
        print(f"{row[0]}: {row[1]} подписчиков, {row[2]} видео")

    conn.close()

def export_channel_contacts(output_file="channel_contacts.txt", db_path="onmyoj.db"):
    """Экспорт всех контактов каналов в файл"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
                   SELECT c.title, c.channel_id, c.subscriber_count,
                          cc.contact_type, cc.value
                   FROM channel_contacts cc
                            JOIN channels c ON c.channel_id = cc.channel_id
                   ORDER BY CAST(c.subscriber_count AS INTEGER) DESC
                   """)

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"Экспорт контактов каналов - {datetime.now()}\n")
        f.write("="*60 + "\n\n")

        current_channel = None
        for row in cursor.fetchall():
            if current_channel != row[0]:
                current_channel = row[0]
                f.write(f"\n\nКанал: {current_channel}\n")
                f.write(f"URL: https://youtube.com/channel/{row[1]}\n")
                f.write(f"Подписчиков: {row[2]}\n")
                f.write("Контакты:\n")
            f.write(f"  {row[3]}: {row[4]}\n")

    print(f"Контакты экспортированы в {output_file}")
    conn.close()

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "export":
        export_channel_contacts()
    else:
        analyze_channels()