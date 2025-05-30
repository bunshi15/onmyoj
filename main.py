import os
import asyncio

from dotenv import load_dotenv
from modules.youtube_scraper import search_videos, extract_contacts, get_video_comments
from modules.yt_ch_scraper import get_channel_info
from modules.save_report import init_db, save_video, save_contact, save_contact_comments

load_dotenv()
API_KEY = os.getenv("API_KEY")



async def process_video(r):
    await save_video(r)
    contacts = extract_contacts(r['description_snippet'])
    for k, vals in contacts.items():
        for v in vals:
            await save_contact(r['video_id'], k, v)
    comments = get_video_comments(r['video_id'], API_KEY, max_comments=20)
    for com, vals in comments:
        for v in vals:
            await  save_contact_comments(r['video_id'], com, v)
    # Аналогично — для комментариев

async def main():
    await init_db()
    results = search_videos(...)
    tasks = [process_video(r) for r in results]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    main()
