import feedparser
import httpx
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import asyncio

def clean_html(html_content: str) -> str:
    if not html_content:
        return ""
    
    soup = BeautifulSoup(html_content, "html.parser")
    
    for tag in soup(["script", "style", "img"]):
        tag.decompose()
    
    lines = (line.strip() for line in soup.get_text().splitlines())
    
    return "\n".join(line for line in lines if line)

async def scrape_rss_source(source: dict) -> list[dict]:
    print(f"Scraping RSS source: {source['name']} - {source['url']}")

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(source["url"], headers={"User-Agent": "Mozilla/5.0 (compatible; BlogPipeline/1.0)"}, follow_redirects=True)
            response.raise_for_status()
        feed = feedparser.parse(response.text)

        articles = []

        for entry in feed.entries:
            content = ""
            if hasattr(entry, "content"):
                content = entry.content[0].value
            elif hasattr(entry, "summary"):
                content = entry.summary
            cleaned_content = clean_html(content)

            if len(cleaned_content) < 100:
                continue

            published_at = None
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                published_at = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).isoformat()
            
            articles.append({
                "title": entry.get("title", "").strip(),
                "url": entry.get("link", ""),
                "content": cleaned_content,
                "excerpt": cleaned_content[:500] + "..." if len(cleaned_content) > 500 else cleaned_content,
                "source_name": source["name"],
                "category": source["category"],
                "tags": source["tags"],
                "published_at": published_at,
                "scraped_at": datetime.now(timezone.utc).isoformat()
            })

        print(f"Scraped {len(articles)} articles from {source['name']}")
        return articles
    except httpx.TimeoutException as e:
        print(f"Timeout while scraping {source['name']}: {e}")
        return []

    except httpx.RequestError as e:
        print(f"Network error while scraping {source['name']}: {e}")
        return []

    except httpx.HTTPStatusError as e:
        print(f"HTTP error while scraping {source['name']}: {e}")
        return []

    except Exception as e:
        print(f"Error scraping {source['name']}: {e}")
        return []

async def scrape_all_sources(sources: list[dict]) -> list[dict]:
    print(f"Starting to scrape {len(sources)} sources...")
    tasks = [scrape_rss_source(source) for source in sources]
    results = await asyncio.gather(*tasks)
    all_articles = [article for sublist in results for article in sublist]
    print(f"Finished scraping. Total articles scraped: {len(all_articles)}")
    return all_articles