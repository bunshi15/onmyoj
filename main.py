import os
import asyncio
import uuid
import typer

from dotenv import load_dotenv
from googleapiclient.errors import HttpError

from modules import db_cli, report_export
from modules.youtube_scraper import search_videos, extract_contacts, get_video_comments
from modules.yt_ch_scraper import get_channel_info
from modules.save_report import (
    init_db, save_video, save_contact,
    save_comment, save_comment_contact,
    save_channel, save_channel_contact, create_session
)

# Загружаем переменные окружения
load_dotenv()
API_KEY = os.getenv("API_KEY")

# Глобальная переменная для пути к БД
DB_PATH = "onmyoj.db"

app = typer.Typer()

@app.command()
def collect(query: str = None):
    """Собрать и сохранить данные по запросу"""
    asyncio.run(main_collect(query))

@app.command()
def list_sessions():
    """Показать список сессий"""
    db_cli.list_sessions()

@app.command()
def use_session(session_id: int):
    """Выбрать сессию активной"""
    db_cli.use_session(session_id)

@app.command()
def show_videos(limit: int =  typer.Option(10), session_id: int = None):
    db_cli.show_videos(limit=limit, session_id=session_id)

@app.command()
def show_channels(limit: int =  typer.Option(10), min_subs: int = typer.Option(0), session_id: int = None):
    db_cli.show_channels(limit=limit, min_subs=min_subs, session_id=session_id)

@app.command()
def search_contacts(
        contact_type: str = None,
        search_term: str = None,
        source: str = typer.Option("all"),  # 'all', 'video', 'comment', 'channel'
        limit: int =  typer.Option(20),
        session_id: int = None):
    db_cli.search_contacts(contact_type=contact_type, search_term=search_term, source=source, limit=limit, session_id=session_id)

@app.command()
def analyze_channels(session_id: int = None):
    db_cli.analyze_channels(session_id=session_id)

@app.command()
def export_report(fmt: str = typer.Option("md"), out_path: str = None):
    """Экспорт отчёта по видео (md/html/csv)"""
    session_info = db_cli.current_session()
    if session_info is None or len(session_info) == 0:
        print("Нет активной сессии.")
        return
    if not out_path:
        print(f"Current folder: {os.getcwd()}")
        out_path = os.getcwd() + '/report.' + fmt
        print(f"Path to report: {out_path}")
    report_export.export_session_report(session_info, db_path=DB_PATH, fmt=fmt, out_path=out_path)

@app.command()
def quicksearch(query: str = None):
    """Собрать и сразу показать статистику по запросу"""
    asyncio.run(main_collect(query))
    db_cli.stats()

async def process_channel(channel_id, session_id):
    """Обработка канала"""
    if not API_KEY:
        return

    try:
        channel_info = get_channel_info(channel_id, API_KEY)
        if channel_info:
            # Сохраняем информацию о канале
            await save_channel(channel_info, session_id, DB_PATH)
            print(f"  Канал: {channel_info['title']} ({channel_info['subscriber_count']} подписчиков)")

            # Сохраняем контакты из описания канала
            for contact_type, values in channel_info['contacts'].items():
                for value in values:
                    await save_channel_contact(channel_id, session_id, contact_type, value, DB_PATH)
                    print(f"    Контакт канала - {contact_type}: {value}")
    except Exception as e:
        print(f"Ошибка при обработке канала {channel_id}: {e}")

async def process_video(session_id, video_data):
    """Обработка одного видео"""
    try:
        # Сохраняем информацию о видео
        await save_video(video_data, session_id, DB_PATH)
        print(f"Сохранено видео: {video_data['title']}")

        # Обрабатываем канал
        if video_data['channel_id']:
            await process_channel(video_data['channel_id'], session_id)

        # Извлекаем и сохраняем контакты из описания
        if video_data['description_snippet']:
            contacts = extract_contacts(video_data['description_snippet'])
            for contact_type, values in contacts.items():
                for value in values:
                    await save_contact(video_data['video_id'], session_id, contact_type, value, DB_PATH)
                    print(f"  Найден {contact_type}: {value}")

        # Получаем и обрабатываем комментарии
        if API_KEY:
            try:
                comments = get_video_comments(video_data['video_id'], API_KEY, max_comments=20)
                print(f"  Найдено комментариев: {len(comments)}")

                for comment_data in comments:
                    # Сохраняем комментарий
                    comment_id = await save_comment(
                        video_data['video_id'],
                        session_id,
                        comment_data['author'],
                        comment_data['comment'],
                        DB_PATH
                    )

                    # Сохраняем контакты из комментария
                    for contact_type, values in comment_data['contacts'].items():
                        for value in values:
                            await save_comment_contact(comment_id, session_id, contact_type, value, DB_PATH)
                            print(f"    Контакт из комментария - {contact_type}: {value}")
            except HttpError as e:
                if e.resp.status == 403 and "commentsDisabled" in str(e):
                    print("  Комментарии отключены — пропускаем.")
                else:
                    print(f"  Ошибка при получении комментариев: {e}")
        else:
            print("  API_KEY не найден - пропускаем комментарии")

    except Exception as e:
        print(f"Ошибка при обработке видео {video_data['video_id']}: {e}")
        import traceback
        traceback.print_exc()

async def main_collect(query):
    """Главная функция"""
    # Проверяем текущую директорию
    print(f"Текущая директория: {os.getcwd()}")
    print(f"Путь к БД: {os.path.abspath(DB_PATH)}")

    # Инициализируем базу данных
    await init_db(DB_PATH)
    print("База данных инициализирована")

    # Проверяем, что файл создан
    if os.path.exists(DB_PATH):
        print(f"БД создана успешно, размер: {os.path.getsize(DB_PATH)} байт")
    else:
        print("ОШИБКА: Файл БД не создан!")

    comment = str(uuid.uuid4())
    session_id = await create_session(query, comment=comment, db_path=DB_PATH)
    # Ищем видео
    print(f"\nНачата сессия: {session_id} '{comment}'")
    print(f"\nПоиск видео по запросу: '{query}'")
    results = search_videos(query, limit=10)
    print(f"Найдено видео: {len(results)}")

    if not results:
        print("Видео не найдены")
        return

    # Обрабатываем видео последовательно, чтобы избежать блокировок БД
    print("\nОбработка видео...")
    for i, video in enumerate(results, 1):
        print(f"\n--- Видео {i}/{len(results)} ---")
        await process_video(session_id, video)

    # Закрываем подключение к БД
    from modules.save_report import close_db
    await close_db()

    print(f"\nОбработка завершена. Обработано видео: {len(results)}")

    db_cli.set_current_session(session_id)
    print(f"\nСохранено. текущая сессия: session_id:{session_id} '{comment}'")

if __name__ == "__main__":
    app()