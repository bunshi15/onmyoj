# modules/save_report.py
import aiosqlite
from contextlib import asynccontextmanager

# Глобальное подключение к БД
_db_connection = None

@asynccontextmanager
async def get_db(db_path="onmyoj.db"):
    """Получение подключения к БД"""
    global _db_connection
    if _db_connection is None:
        _db_connection = await aiosqlite.connect(db_path)
        # Включаем WAL mode для лучшей работы с конкурентностью
        await _db_connection.execute("PRAGMA journal_mode=WAL")
        await _db_connection.execute("PRAGMA busy_timeout=30000")  # 30 секунд таймаут
    yield _db_connection

async def close_db():
    """Закрытие подключения к БД"""
    global _db_connection
    if _db_connection:
        await _db_connection.close()
        _db_connection = None

async def init_db(db_path="onmyoj.db"):
    """Инициализация базы данных"""
    async with get_db(db_path) as db:
        # Таблица видео
        await db.execute("""
                         CREATE TABLE IF NOT EXISTS videos (
                                                               video_id TEXT PRIMARY KEY,
                                                               title TEXT,
                                                               url TEXT,
                                                               channel_name TEXT,
                                                               channel_id TEXT,
                                                               published_time TEXT,
                                                               view_count TEXT,
                                                               duration TEXT,
                                                               description_snippet TEXT
                         )
                         """)

        # Таблица контактов из описаний видео
        await db.execute("""
                         CREATE TABLE IF NOT EXISTS video_contacts (
                                                                       id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                       video_id TEXT,
                                                                       contact_type TEXT,
                                                                       value TEXT,
                                                                       FOREIGN KEY(video_id) REFERENCES videos(video_id)
                             )
                         """)

        # Таблица каналов
        await db.execute("""
                         CREATE TABLE IF NOT EXISTS channels (
                                                                 channel_id TEXT PRIMARY KEY,
                                                                 title TEXT,
                                                                 description TEXT,
                                                                 published_at TEXT,
                                                                 country TEXT,
                                                                 view_count TEXT,
                                                                 subscriber_count TEXT,
                                                                 video_count TEXT,
                                                                 last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                         )
                         """)

        # Таблица комментариев
        await db.execute("""
                         CREATE TABLE IF NOT EXISTS comments (
                                                                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                 video_id TEXT,
                                                                 author TEXT,
                                                                 comment TEXT,
                                                                 FOREIGN KEY(video_id) REFERENCES videos(video_id)
                             )
                         """)

        # Таблица контактов из комментариев
        await db.execute("""
                         CREATE TABLE IF NOT EXISTS comment_contacts (
                                                                         id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                         comment_id INTEGER,
                                                                         contact_type TEXT,
                                                                         value TEXT,
                                                                         FOREIGN KEY(comment_id) REFERENCES comments(id)
                             )
                         """)

        # Таблица контактов из каналов
        await db.execute("""
                         CREATE TABLE IF NOT EXISTS channel_contacts (
                                                                         id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                         channel_id TEXT,
                                                                         contact_type TEXT,
                                                                         value TEXT,
                                                                         FOREIGN KEY(channel_id) REFERENCES channels(channel_id)
                             )
                         """)

        await db.commit()

async def save_video(video, db_path="onmyoj.db"):
    """Сохранение информации о видео"""
    async with get_db(db_path) as db:
        await db.execute("""
            INSERT OR REPLACE INTO videos (
                video_id, title, url, channel_name, channel_id,
                published_time, view_count, duration, description_snippet
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            video['video_id'],
            video['title'],
            video['url'],
            video['channel_name'],
            video['channel_id'],
            video['published_time'],
            video['view_count'],
            video['duration'],
            video['description_snippet']
        ))
        await db.commit()

async def save_contact(video_id, contact_type, value, db_path="onmyoj.db"):
    """Сохранение контакта из описания видео"""
    async with get_db(db_path) as db:
        await db.execute("""
                         INSERT INTO video_contacts (video_id, contact_type, value)
                         VALUES (?, ?, ?)
                         """, (video_id, contact_type, value))
        await db.commit()

async def save_comment(video_id, author, comment_text, db_path="onmyoj.db"):
    """Сохранение комментария и возврат его ID"""
    async with get_db(db_path) as db:
        cursor = await db.execute("""
                                  INSERT INTO comments (video_id, author, comment)
                                  VALUES (?, ?, ?)
                                  """, (video_id, author, comment_text))
        await db.commit()
        return cursor.lastrowid

async def save_comment_contact(comment_id, contact_type, value, db_path="onmyoj.db"):
    """Сохранение контакта из комментария"""
    async with get_db(db_path) as db:
        await db.execute("""
                         INSERT INTO comment_contacts (comment_id, contact_type, value)
                         VALUES (?, ?, ?)
                         """, (comment_id, contact_type, value))
        await db.commit()

async def save_channel(channel_info, db_path="onmyoj.db"):
    """Сохранение информации о канале"""
    async with get_db(db_path) as db:
        await db.execute("""
            INSERT OR REPLACE INTO channels (
                channel_id, title, description, published_at, 
                country, view_count, subscriber_count, video_count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            channel_info['channel_id'],
            channel_info['title'],
            channel_info['description'],
            channel_info['published_at'],
            channel_info['country'],
            channel_info['view_count'],
            channel_info['subscriber_count'],
            channel_info['video_count']
        ))
        await db.commit()

async def save_channel_contact(channel_id, contact_type, value, db_path="onmyoj.db"):
    """Сохранение контакта из описания канала"""
    async with get_db(db_path) as db:
        await db.execute("""
                         INSERT INTO channel_contacts (channel_id, contact_type, value)
                         VALUES (?, ?, ?)
                         """, (channel_id, contact_type, value))
        await db.commit()