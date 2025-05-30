import asyncio
import os
import pytest
from modules.save_report import init_db, save_video, save_comment, save_channel, save_contact, save_comment_contact

DB_PATH = "test_onmyoj.db"

@pytest.fixture(scope="module", autouse=True)
def cleanup_db():
    # Перед тестом — удалить, чтобы был чистый старт
    try:
        os.remove(DB_PATH)
    except FileNotFoundError:
        pass
    yield
    try:
        os.remove(DB_PATH)
    except FileNotFoundError:
        pass

@pytest.mark.asyncio
async def test_full_save_report_flow():
    await init_db(DB_PATH)
    video = {
        "video_id": "testid123",
        "title": "Test title",
        "url": "http://yt.com/testid123",
        "channel_name": "Test Channel",
        "channel_id": "chan_321",
        "published_time": "2022-02-02",
        "view_count": "1000",
        "duration": "10:00",
        "description_snippet": "test description"
    }
    await save_video(video, DB_PATH)
    await save_contact("testid123", "telegram", "t.me/test", DB_PATH)
    comment_id = await save_comment("testid123", "user1", "test comment", DB_PATH)
    await save_comment_contact(comment_id, "discord", "discord.gg/abc", DB_PATH)
    await save_channel("chan_321", "Test Channel", "desc", "2022-02-02", "RU", "100", "20", "10", DB_PATH)

    # Проверяем что все сохранилось
    import aiosqlite
    async with aiosqlite.connect(DB_PATH) as db:
        # Видео
        cur = await db.execute("SELECT * FROM videos WHERE video_id = ?", ("testid123",))
        v = await cur.fetchone()
        assert v is not None and v[0] == "testid123"
        # Контакт
        cur = await db.execute("SELECT * FROM video_contacts WHERE video_id = ?", ("testid123",))
        contacts = await cur.fetchall()
        assert any("t.me/test" in str(c) for c in contacts)
        # Комментарий
        cur = await db.execute("SELECT * FROM comments WHERE id = ?", (comment_id,))
        comment = await cur.fetchone()
        assert comment is not None and "test comment" in comment[3]
        # Комментарий-контакт
        cur = await db.execute("SELECT * FROM comment_contacts WHERE comment_id = ?", (comment_id,))
        ccontact = await cur.fetchone()
        assert ccontact is not None and "discord.gg/abc" in ccontact[3]
        # Канал
        cur = await db.execute("SELECT * FROM channels WHERE channel_id = ?", ("chan_321",))
        ch = await cur.fetchone()
        assert ch is not None and ch[1] == "Test Channel"

