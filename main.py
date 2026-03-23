import asyncio
from scraper.rss import scrape_all_sources
from scraper.sources import SOURCES
from database.db import save_articles

async def main():
    print("Starting the scraping process...")

    articles = await scrape_all_sources(SOURCES)

    print(f"Total articles scraped: {len(articles)}")

    result = save_articles(articles)

    print(f"Articles saved: {result['saved']}, Errors: {result['errors']}")

if __name__ == "__main__":
    asyncio.run(main())