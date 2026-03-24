import asyncio
from scraper.rss import scrape_all_sources
from scraper.sources import SOURCES
from database.db import save_articles
from embeddings.generator import generate_embeddings_for_all

async def main():
    print("Starting the scraping process...")

    articles = await scrape_all_sources(SOURCES)

    print(f"Total articles scraped: {len(articles)}")

    result = save_articles(articles)

    print(f"Articles saved: {result['saved']}, Errors: {result['errors']}")
    
    print("\nGenerating embeddings for articles...")
    generate_embeddings_for_all()

if __name__ == "__main__":
    asyncio.run(main())