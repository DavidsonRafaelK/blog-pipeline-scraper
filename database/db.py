import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL harus di-set di file .env!")
    return psycopg2.connect(database_url)

def save_articles(articles: list[dict]) -> dict:
    if not articles:
        print("Tidak ada artikel baru untuk disimpan.")
        return {"saved": 0, "errors": 0}

    saved_count = 0
    error_count = 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            batch_size = 50
            for i in range(0, len(articles), batch_size):
                batch = articles[i:i + batch_size]
                try:
                    values = [
                        (
                            a["title"],
                            a["url"],
                            a["content"],
                            a["excerpt"],
                            a["source_name"],
                            a["category"],
                            a["tags"],
                            a.get("published_at"),
                            a["scraped_at"],
                        )
                        for a in batch
                    ]
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO scraped_articles 
                            (title, url, content, excerpt, source_name, category, tags, published_at, scraped_at)
                        VALUES %s
                        ON CONFLICT (url) 
                        DO UPDATE SET
                            title      = EXCLUDED.title,
                            content    = EXCLUDED.content,
                            excerpt    = EXCLUDED.excerpt,
                            scraped_at = EXCLUDED.scraped_at
                        """,
                        values
                    )
                    conn.commit()
                    saved_count += len(batch)
                    print(f"Saved batch of {len(batch)} articles. Total saved: {saved_count}")
                except Exception as e:
                    conn.rollback()
                    error_count += len(batch)
                    print(f"Error saving batch of {len(batch)} articles: {e}")
                    
    return {"saved": saved_count, "errors": error_count}