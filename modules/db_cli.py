import typer
import sqlite3
import pandas as pd

app = typer.Typer()

DB_PATH = "onmyoj.db"

@app.command()
def show_videos(limit: int = 10):
    """Показать N видео из базы."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM videos LIMIT ?", conn, params=(limit,))
    typer.echo(df)
    conn.close()

@app.command()
def show_channels():
    """Показать все каналы."""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM channels", conn)
    typer.echo(df)
    conn.close()

@app.command()
def search_contact(contact_type: str, value: str):
    """Найти видео/комментарии по контакту (telegram, discord, email, http)"""
    conn = sqlite3.connect(DB_PATH)
    q1 = "SELECT * FROM video_contacts WHERE contact_type=? AND value LIKE ?"
    df = pd.read_sql_query(q1, conn, params=(contact_type, f"%{value}%"))
    typer.echo(df)
    conn.close()

@app.command()
def export_report(fmt: str = "html"):
    """Экспортировать отчет по всем видео (html/csv)"""
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM videos", conn)
    if fmt == "html":
        df.to_html("report.html", index=False)
        typer.echo("Сохранено в report.html")
    elif fmt == "csv":
        df.to_csv("report.csv", index=False)
        typer.echo("Сохранено в report.csv")
    conn.close()

if __name__ == "__main__":
    app()
