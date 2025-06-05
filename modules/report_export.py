# modules/report_export.py

import pandas as pd
import sqlite3
from collections import Counter
import re

def export_session_report(session_info, db_path, fmt="html", out_path=None):

    row = session_info.iloc[0]
    session_id = int(row['session_id'])
    session_uuid = row['comment']
    session_date = row['started_at']
    session_keyword = row['keyword']

    conn = sqlite3.connect(db_path)
    # Видео
    videos = pd.read_sql_query(
        "SELECT video_id, title, url, channel_name, published_time, view_count FROM videos WHERE session_id=?",
        conn, params=(session_id,)
    )
    # Каналы
    channels = pd.read_sql_query(
        "SELECT channel_id, title, subscriber_count, video_count FROM channels WHERE session_id=?",
        conn, params=(session_id,)
    )
    # Контакты (email, tg, и т.д.)
    contacts = pd.read_sql_query("""
                                 SELECT
                                     vc.contact_type,
                                     vc.value,
                                     'video' as source,
                                     v.video_id as object_id,
                                     v.title as object_title
                                 FROM video_contacts vc
                                          JOIN videos v ON v.video_id = vc.video_id
                                 WHERE vc.session_id = ?
                                 UNION ALL
                                 SELECT
                                     cc.contact_type,
                                     cc.value,
                                     'channel' as source,
                                     c.channel_id as object_id,
                                     c.title as object_title
                                 FROM channel_contacts cc
                                          JOIN channels c ON c.channel_id = cc.channel_id
                                 WHERE cc.session_id = ?
                                 UNION ALL
                                 SELECT
                                     cc.contact_type,
                                     cc.value,
                                     'comment' as source,
                                     c.id as object_id,
                                     substr(c.comment, 1, 40) as object_title
                                 FROM comment_contacts cc
                                          JOIN comments c ON c.id = cc.comment_id
                                 WHERE cc.session_id = ?
                                 """, conn, params=(session_id, session_id, session_id))
    conn.close()

    # Секция топ-слов
    all_text = " ".join(videos['title'].fillna("")) + " " + " ".join(videos.get('description_snippet', pd.Series()).fillna(""))
    keywords = extract_keywords(all_text)
    stopwords = {...}
    filtered = [w for w in keywords if w not in stopwords and not w.isdigit()]
    common = Counter(filtered).most_common(10)

    # Собери полную таблицу с object_id/object_title (см. прошлый пример)
    # Далее сгруппируй по contact_type, value и собери источники:
    contact_sources = contacts.groupby(['contact_type', 'value']) \
        .agg({'source': lambda x: ', '.join(sorted(set(x)))}) \
        .reset_index()

    # Добавь количество (frequency)
    contact_sources['count'] = contacts.groupby(['contact_type', 'value']).size().values

    # Оставь только повторяющиеся
    repeated = contact_sources[contact_sources['count'] > 1] \
        .sort_values('count', ascending=False)

    report = ""
    if fmt == "md":
        report += f"# Report {session_id} '{session_uuid}'\n"
        report += f"# Date: {session_date} \n"
        report += f"# Keyword: {session_keyword}\n\n"
        report += "## Top Keywords for Further OSINT\n"
        for word, count in common:
            report += f"- {word}: {count}\n"
        report += "## YT Videos\n" + videos.to_markdown(index=False) + "\n\n"
        report += "## YT Channels\n" + channels.to_markdown(index=False) + "\n\n"
        report += "## Contacts\n" + contacts.to_markdown(index=False) + "\n\n"
        report += "## Repeated contacts (seen in multiple sources)\n"
        if not repeated.empty:
            report += repeated.to_markdown(index=False) + "\n"
        else:
            report += "Нет повторяющихся контактов\n"
    elif fmt == "csv":
        if out_path is None:
            raise ValueError("Set out_path for CSV")
        videos.to_csv(out_path.replace(".csv", "_videos.csv"), index=False)
        channels.to_csv(out_path.replace(".csv", "_channels.csv"), index=False)
        contacts.to_csv(out_path.replace(".csv", "_contacts.csv"), index=False)
        return out_path
    else:  # HTML по умолчанию
        report += f"<h1>Report {session_id} '{session_uuid}'</h1>\n"
        report += f"<h2>Date {session_date}</h2>\n"
        report += f"<h3>Keyword: {session_keyword}</h3>\n"
        report += "<h2>Top Keywords for Further OSINT</h2><ul>"
        for word, count in common:
            report += f"<li>{word}: {count}</li>"
        report += "</ul>"
        report += "<h2>YT Videos</h2>" + videos.to_html(index=False) + "\n"
        report += "<h2>YT Channels</h2>" + channels.to_html(index=False) + "\n"
        report += "<h2>Contacts</h2>" + contacts.to_html(index=False) + "\n"
        report += "<h2>Repeated contacts (seen in multiple sources)</h2>"+ "\n"
        if not repeated.empty:
            report += repeated.to_html(index=False)
        else:
            report += "<p>Нет повторяющихся контактов</p>"
    if out_path and fmt != "csv":
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(report)
        return out_path
    return report

def extract_keywords(text, min_len=4):
    # Берём только буквы и цифры, убираем всё лишнее
    return [w.lower() for w in re.findall(r'\b\w{%d,}\b' % min_len, text)]

