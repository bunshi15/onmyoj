# modules/youtube_scraper.py
import re

from youtubesearchpython import VideosSearch
from googleapiclient.discovery import build

def search_videos(query: str, limit: int = 20):
    videos_search = VideosSearch(query, limit=limit)
    results = videos_search.result()

    extracted = []
    for video in results.get("result", []):
        extracted.append({
            "video_id": video.get("id", ""),
            "title": video.get("title", ""),
            "url": video.get("link", ""),
            "channel_name": video.get("channel", {}).get("name", ""),
            "channel_id": video.get("channel", {}).get("id", ""),
            "channel_link": video.get("channel", {}).get("link", ""),
            "view_count": video.get("viewCount", {}).get("text", ""),
            "published_time": video.get("publishedTime", ""),
            "duration": video.get("duration", ""),
            "description_snippet": " ".join(x["text"] for x in video.get("descriptionSnippet", [])) if video.get("descriptionSnippet") else "",
            "thumbnails": [x.get("url", "") for x in video.get("thumbnails", [])],
            "badges": video.get("badges", []),
            "is_live": video.get("isLive", False),
            "type": video.get("type", ""),
            "accessibility": video.get("accessibility", {}),
            "rich_thumbnail": video.get("richThumbnail", {}).get("url", "") if video.get("richThumbnail") else ""
        })
    return extracted

def extract_contacts(description: str):
    patterns = {
        "telegram": r"(t\.me/[\w\d_]+|@[\w\d_]{4,})",
        "discord": r"(discord(\.gg|\.com/invite|\.app/invite)/[\w\d]+)",
        "email": r"([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)",
        "http": r"(https?://[^\s]+)",
        "pastebin": r"(pastebin\.com/[a-zA-Z0-9]+)"
    }
    results = {}
    for name, pat in patterns.items():
        found = re.findall(pat, description)
        # re.findall может возвращать кортежи, берем всегда первый элемент
        results[name] = [f[0] if isinstance(f, tuple) else f for f in found]
    return results

def get_video_comments(video_id, api_key, max_comments=30):
    youtube = build('youtube', 'v3', developerKey=api_key)
    request = youtube.commentThreads().list(
        part='snippet',
        videoId=video_id,
        maxResults=min(max_comments, 100),
        textFormat='plainText'
    )
    response = request.execute()
    comments = []
    for item in response.get('items', []):
        comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
        author = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
        contacts = extract_contacts(comment)
        comments.append({
            'author': author,
            'comment': comment,
            'contacts': contacts
        })
    return comments