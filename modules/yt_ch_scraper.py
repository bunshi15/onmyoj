# modules/yt_ch_scraper.py
from googleapiclient.discovery import build
from modules.youtube_scraper import extract_contacts

def get_channel_info(channel_id, api_key):
    """Получение информации о канале"""
    try:
        youtube = build('youtube', 'v3', developerKey=api_key)
        request = youtube.channels().list(
            part='snippet,statistics',
            id=channel_id
        )
        response = request.execute()

        if response['items']:
            item = response['items'][0]
            info = {
                'channel_id': channel_id,
                'title': item['snippet']['title'],
                'description': item['snippet'].get('description', ''),
                'published_at': item['snippet'].get('publishedAt', ''),
                'country': item['snippet'].get('country', ''),
                'view_count': item['statistics'].get('viewCount', '0'),
                'subscriber_count': item['statistics'].get('subscriberCount', '0'),
                'video_count': item['statistics'].get('videoCount', '0'),
            }

            # Извлекаем контакты из описания канала
            if info['description']:
                info['contacts'] = extract_contacts(info['description'])
            else:
                info['contacts'] = {}

            return info
        return None
    except Exception as e:
        print(f"Error getting channel info for {channel_id}: {e}")
        return None