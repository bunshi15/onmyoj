# modules/save_report.py
import aiosqlite

async def init_db(db_path="onmyoj.db"):
    async with aiosqlite.connect(db_path) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS videos (
                                                                  video_id TEXT PRIMARY KEY,
                                                                  title TEXT, url TEXT, channel_name TEXT, channel_id TEXT,
                                                                  published_time TEXT, view_count TEXT, duration TEXT, description_snippet TEXT
                            )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS video_contacts (
                                                                          id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                          video_id TEXT, contact_type TEXT, value TEXT,
                                                                          FOREIGN KEY(video_id) REFERENCES videos(video_id)
            )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS comments (
                                                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                    video_id TEXT, author TEXT, comment TEXT,
                                                                    FOREIGN KEY(video_id) REFERENCES videos(video_id)
            )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS comment_contacts (
                                                                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                            comment_id INTEGER, contact_type TEXT, value TEXT,
                                                                            FOREIGN KEY(comment_id) REFERENCES comments(id)
            )""")
        await db.commit()

async def save_video(video, db_path="onmyoj.db"):
    async with aiosqlite.connect(db_path) as db:
        await db.execute("""
            INSERT OR REPLACE INTO videos (video_id, title, url, channel_name, channel_id,
            published_time, view_count, duration, description_snippet)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            video['video_id'], video['title'], video['url'], video['channel_name'],
            video['channel_id'], video['published_time'], video['view_count'],
            video['duration'], video['description_snippet']
        ))
        await db.commit()

async def save_contact(video_id, contact_type, value, db_path="onmyoj.db"):
    async with aiosqlite.connect(db_path) as db:
        await db.execute("""
                         INSERT INTO video_contacts (video_id, contact_type, value)
                         VALUES (?, ?, ?)
                         """, (video_id, contact_type, value))
        await db.commit()

async def save_contact_comments(video_id, contact_type, value, db_path="onmyoj.db"):
    async with aiosqlite.connect(db_path) as db:
        await db.execute("""
                         INSERT INTO comment_contacts (video_id, contact_type, value)
                         VALUES (?, ?, ?)
                         """, (video_id, contact_type, value))
        await db.commit()

