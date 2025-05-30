import os
import asyncio
from dotenv import load_dotenv

from modules.youtube_scraper import search_videos, extract_contacts, get_video_comments
from modules.yt_ch_scraper import get_channel_info
from modules.save_report import (
    init_db, save_video, save_contact,
    save_comment, save_comment_contact,
    save_channel, save_channel_contact
)

# Загружаем переменные окружения
load_dotenv()
API_KEY = os.getenv("API_KEY")

# Глобальная переменная для пути к БД
DB_PATH = "onmyoj.db"

async def process_channel(channel_id):
    """Обработка канала"""
    if not API_KEY:
        return

    try:
        channel_info = get_channel_info(channel_id, API_KEY)
        if channel_info:
            # Сохраняем информацию о канале
            await save_channel(channel_info, DB_PATH)
            print(f"  Канал: {channel_info['title']} ({channel_info['subscriber_count']} подписчиков)")

            # Сохраняем контакты из описания канала
            for contact_type, values in channel_info['contacts'].items():
                for value in values:
                    await save_channel_contact(channel_id, contact_type, value, DB_PATH)
                    print(f"    Контакт канала - {contact_type}: {value}")
    except Exception as e:
        print(f"Ошибка при обработке канала {channel_id}: {e}")

async def process_video(video_data):
    """Обработка одного видео"""
    try:
        # Сохраняем информацию о видео
        await save_video(video_data, DB_PATH)
        print(f"Сохранено видео: {video_data['title']}")

        # Обрабатываем канал
        if video_data['channel_id']:
            await process_channel(video_data['channel_id'])

        # Извлекаем и сохраняем контакты из описания
        if video_data['description_snippet']:
            contacts = extract_contacts(video_data['description_snippet'])
            for contact_type, values in contacts.items():
                for value in values:
                    await save_contact(video_data['video_id'], contact_type, value, DB_PATH)
                    print(f"  Найден {contact_type}: {value}")

        # Получаем и обрабатываем комментарии
        if API_KEY:
            comments = get_video_comments(video_data['video_id'], API_KEY, max_comments=20)
            print(f"  Найдено комментариев: {len(comments)}")

            for comment_data in comments:
                # Сохраняем комментарий
                comment_id = await save_comment(
                    video_data['video_id'],
                    comment_data['author'],
                    comment_data['comment'],
                    DB_PATH
                )

                # Сохраняем контакты из комментария
                for contact_type, values in comment_data['contacts'].items():
                    for value in values:
                        await save_comment_contact(comment_id, contact_type, value, DB_PATH)
                        print(f"    Контакт из комментария - {contact_type}: {value}")
        else:
            print("  API_KEY не найден - пропускаем комментарии")

    except Exception as e:
        print(f"Ошибка при обработке видео {video_data['video_id']}: {e}")
        import traceback
        traceback.print_exc()

async def main():
    """Главная функция"""
    # Проверяем текущую директорию
    import os
    print(f"Текущая директория: {os.getcwd()}")
    db_path = "onmyoj.db"
    print(f"Путь к БД: {os.path.abspath(db_path)}")

    # Инициализируем базу данных
    await init_db(DB_PATH)
    print("База данных инициализирована")

    # Проверяем, что файл создан
    if os.path.exists(DB_PATH):
        print(f"БД создана успешно, размер: {os.path.getsize(DB_PATH)} байт")
    else:
        print("ОШИБКА: Файл БД не создан!")

    # Поисковый запрос - ИЗМЕНИТЕ НА СВОЙ
    search_query = "xworm Vbs Crypter Fud Bypass Windows Defender EXE Payload"  # Замените на нужный запрос

    # Ищем видео
    print(f"\nПоиск видео по запросу: '{search_query}'")
    results = search_videos(search_query, limit=10)
    print(f"Найдено видео: {len(results)}")

    if not results:
        print("Видео не найдены")
        return

    # Обрабатываем видео последовательно, чтобы избежать блокировок БД
    print("\nОбработка видео...")
    for i, video in enumerate(results, 1):
        print(f"\n--- Видео {i}/{len(results)} ---")
        await process_video(video)

    # Закрываем подключение к БД
    from modules.save_report import close_db
    await close_db()

    print(f"\nОбработка завершена. Обработано видео: {len(results)}")

if __name__ == "__main__":
    # Запускаем асинхронную функцию
    asyncio.run(main())