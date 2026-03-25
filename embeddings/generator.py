import os
import time
import psycopg2
import psycopg2.extras
import google.generativeai as genai

def load_api_key():
    keys = []
    i = 1

    while True:
        key = os.getenv(f'GEMINI_API_KEY_{i}')
        if not key:
            break
        keys.append(key)
        i += 1
    
    if not keys:
        raise ValueError("No API keys found. Please set GEMINI_API_KEY_1, GEMINI_API_KEY_2, etc. in your environment variables.")
    
    print(f"Loaded {len(keys)} API keys.")
    return keys

class GeminiEmbedder:
    def __init__(self, api_keys):
        self.api_keys = api_keys
        self.current_key_index = 0

    def _configure_current_key(self):
        genai.configure(api_key=self.api_keys[self.current_key_index])

    def _rotate_key(self):
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        self._configure_current_key()
        print(f"Switched to API key index: {self.current_key_index}")

    def get_embedding(self, text):
        text = text.replace("\n", " ").strip()
        text = text[:8000]

        attempts = 0
        max_attempts = len(self.api_keys)
        retries = 0
        max_retries = 3

        while attempts < max_attempts:
            try:
                self._configure_current_key()
                result = genai.embed_content(
                    model="models/gemini-embedding-001",
                    content=text,
                    task_type="RETRIEVAL_DOCUMENT",
                    output_dimensionality=768
                )

                return result['embedding']

            except Exception as e:
                error_message = str(e).lower()

                if "429" in error_message or "rate limit" in error_message:
                    print(f"Rate limit hit with key index {self.current_key_index}. Rotating key...")
                    attempts += 1
                    if attempts < max_attempts:
                        self._rotate_key()
                    else:
                        retries += 1
                        if retries >= max_retries:
                            raise Exception("Failed to get embedding: all API keys exhausted after multiple retries.")
                        print(f"All keys exhausted. Waiting 60s before retry {retries}/{max_retries}...")
                        time.sleep(60)
                        attempts = 0
                else:
                    raise e
        raise Exception("Failed to get embedding after exhausting all API keys.")

def generate_embeddings_for_all():
    api_keys = load_api_key()
    embedder = GeminiEmbedder(api_keys)

    with psycopg2.connect(os.environ.get("DATABASE_URL")) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT id, title, content 
                FROM scraped_articles 
                WHERE embedding IS NULL
                ORDER BY scraped_at DESC
            """)

            articles = cur.fetchall()

            if not articles:
                print("No articles found without embeddings.")
                return

            print(f"Found {len(articles)} articles without embeddings. Generating embeddings...")

            success_count = 0
            error_count = 0

            for i, article in enumerate(articles):
                try:
                    text_to_embed = f"{article['title']}\n\n{article['content']}"
                    embedding = embedder.get_embedding(text_to_embed)

                    cur.execute("""
                        UPDATE scraped_articles 
                        SET embedding = %s::vector
                        WHERE id = %s
                    """, (str(embedding), str(article['id'])))

                    conn.commit()
                    success_count += 1
                    print(f"Processed article {i+1}/{len(articles)} (ID: {article['id']}) - Success")
                    time.sleep(0.5)

                except Exception as e:
                    conn.rollback()
                    error_count += 1
                    print(f"Error processing article {i+1}/{len(articles)} (ID: {article['id']}): {e}")

            print(f"Embedding generation completed. Success: {success_count}, Errors: {error_count}")
