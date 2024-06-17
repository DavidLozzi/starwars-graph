import psycopg2
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv
from psycopg2 import sql, pool
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import asyncio
import datetime
import httpx
import os
import random
import redis
import time

load_dotenv()
password = os.getenv("DB_PASSWORD")
postgres_srv = os.getenv("POSTGRES_SRV")
redis_srv = os.getenv("REDIS_SRV")

base_url = "https://starwars.fandom.com/"
start_url = "https://starwars.fandom.com/sitemap-newsitemapxml-index.xml"


ignore_urls = [
    "/wiki/Special:",
    "/wiki/User_talk:",
    "/wiki/Template:",
    "/wiki/Template_talk:",
    "/wiki/Help:",
    "/wiki/User:",
    "/wiki/UserProfile:",
    "/register",
    "/signin",
    "/reset-password",
]


db_params = {
    "database": "star_wars_data",
    "user": "postgres",
    "password": password,
    "host": postgres_srv,
    "port": "5432",
}

conn_pool = psycopg2.pool.SimpleConnectionPool(1, 1000, **db_params)

redis_conn = redis.Redis(host=redis_srv, port=6379, db=0, decode_responses=True)


def run_sql(sql_command, data=None):
    try:
        conn = conn_pool.getconn()
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        if data is not None:
            # print("data is not None")
            cursor.execute(sql_command, data)
        else:
            # print("data is None")
            cursor.execute(sql_command)
        result = cursor.fetchall()
        # print(result)
        cursor.close()

        # print("psql completed")
        return result
    except Exception as e:
        print(f"A database error occurred: {e}")
    finally:
        conn_pool.putconn(conn)


checked_urls = set()


def url_exists(url):
    global redis_conn

    if ".xml" in url:  # always process sitemaps
        print("  URL is a sitemap", url)
        return False

    if url in checked_urls:
        print("  URL exists in checked", url)
        return True

    exists = redis_conn.sismember("processed_urls", url)
    if exists:
        print("  URL exists in cache", url)
        checked_urls.add(url)
        return True

    select_query = "SELECT URL FROM all_data WHERE URL = %s"
    result = run_sql(select_query, (url,))
    if len(result) > 0:
        print("  URL exists in database", url)
        add_url_to_checked(url)
        return True

    print("  URL does not exist", url)
    return False


def add_url_to_checked(url):
    global redis
    try:
        redis_conn.sadd("processed_urls", url)
        checked_urls.add(url)
        print("  URL added to cache and checked_urls", url)
    except Exception as e:
        print(f"Error adding URL to checked: {e}")


def preload_checked_urls():
    postgres_urls = run_sql("SELECT DISTINCT URL FROM all_data")
    inserted_count = redis_conn.scard("processed_urls")
    read_count = 0
    print(f"Loading {len(postgres_urls)} URLs from database to cache")

    # Prepare to batch insert in chunks of 1000 URLs
    batch_size = 5000
    url_batch = []

    for (url,) in postgres_urls:
        url_batch.append(url)
        read_count += 1

        # Insert in batches
        if len(url_batch) >= batch_size:
            added_count = redis_conn.sadd("processed_urls", *url_batch)
            inserted_count += added_count
            checked_urls.update(url_batch)
            url_batch = []  # Reset batch

            print(f"Read {read_count} URLs from {len(postgres_urls)} records.")
            print(f"Cached {inserted_count} URLs from {len(postgres_urls)} records.")

    # Insert any remaining URLs in the batch
    if url_batch:
        added_count = redis_conn.sadd("processed_urls", *url_batch)
        inserted_count += added_count
        checked_urls.update(url_batch)

    print(f"Preloaded {inserted_count} URLs from {len(postgres_urls)} records.")


total_processed = 0
total_added = 0
total_skipped = 0
errors_urls = []


def write_error_to_file(url, error, error_file):
    with open(error_file, "a") as f:
        f.write(f"{url}::{error}\n")


async def fetch(url, error_file, retry=0):
    global errors_urls
    try:
        async with httpx.AsyncClient(follow_redirects=True, max_redirects=20) as client:
            resp = await client.get(url)
            print(f"  Fetched {url} with status code {resp.status_code}")
            if resp.status_code == 200:
                content_type = resp.headers.get("Content-Type", "")
                print(f"  Content type: {content_type}")
                if "xml" in content_type:
                    return (resp, "xml")
                elif "html" in content_type:
                    return (resp, "html")
                else:
                    print(f"Unknown content type for {url}: {content_type}")
                    return (resp, "html")

            else:
                print(f"Error fetching {url}: {resp.status_code}")
                raise Exception(f"{url} {resp.status_code}")
    except Exception as e:
        print(f"FETCH ERROR {url}\n{str(e)}")
        write_error_to_file(url, e, error_file)
        if retry < 2:
            print("RETRYING FETCH", url, retry)
            retry += 1
            await asyncio.sleep(1)
            return fetch(url, error_file, retry)

    return (None, None)


async def scrape(url, base_url, error_file="errors.txt", full_crawl=True):
    global total_processed
    global total_added
    global total_skipped
    print(
        f"\nTotal checked: {len(checked_urls)}, processed: {total_processed} in {time.time() - start_time}s (avg {total_processed/(time.time() - start_time)}/s), added: {total_added}, skipped: {total_skipped}"
    )
    response = None
    # if url_exists(url) is False:
    try:
        (response, content_type) = await fetch(url, error_file)
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        write_error_to_file(url, e, error_file)
        return

    print("  Processing:", url)
    if response is None:
        return

    links = []
    if content_type == "xml":
        soup = BeautifulSoup(response.text, "lxml-xml")
        if full_crawl:
            links = [loc.text for loc in soup.find_all("loc")]
        else:
            date_threshold = datetime.strptime("2024-04-22", "%Y-%m-%d")
            links = [
                url.loc.text
                for url in soup.find_all("url")
                if datetime.strptime(url.lastmod.text, "%Y-%m-%dT%H:%M:%SZ")
                >= date_threshold
            ]
    else:
        soup = BeautifulSoup(response.text, "html.parser")
        body = soup.find("body")

        h1_tags = body.find_all(id="firstHeading")
        title = h1_tags[0].text.strip() if h1_tags else "No title"
        content = str(body).replace("\n", "").replace("\t", "").replace("\r", "")

        store_data(title, url, content)

        total_processed += 1

    tasks = []
    print("processing links", len(links))
    for link in links:
        url_clean = link.split("?")[0] if "?" in link else link
        url_clean = url_clean if len(url_clean) <= 250 else url_clean[:250]
        if (
            url_clean != url
            and not any(ignore in url_clean for ignore in ignore_urls)
            and base_url in url_clean
        ):
            await asyncio.sleep(0.01)
            tasks.append(scrape(url_clean, base_url, error_file, full_crawl))
            print(url_clean)
            if len(tasks) == 15:
                print(f"Gathering tasks for {url}, count: {len(tasks)}")
                await asyncio.gather(*tasks)
                tasks = []
                random_sleep = random.uniform(0.3, 0.6)
                await asyncio.sleep(random_sleep)
            else:
                total_skipped += 1

    if tasks:
        print(f"Final gathering tasks for {url}, count: {len(tasks)}")
        await asyncio.gather(*tasks)
        tasks = []

    soup.decompose()
    soup = None
    print(f"Finished processing {url}")
    print(f"Elapsed time: {time.time() - start_time}")


def store_data(title, url, content):
    global total_added
    if not url_exists(url):
        postgres_insert_query = "INSERT INTO all_data (Title, URL, Content, Last_Ingested) VALUES (%s,%s,%s,%s) RETURNING *"
        record_to_insert = (title, url, content, datetime.datetime.now())
        run_sql(postgres_insert_query, record_to_insert)
        print("  Added URL:", url)
        total_added += 1
        add_url_to_checked(url)
    else:
        print("  URL already exists in database", url)
        postgres_update_query = "UPDATE all_data SET Title = %s, Content = %s, Last_Ingested = %s WHERE URL = %s RETURNING *"
        record_to_update = (title, content, datetime.datetime.now(), url)
        run_sql(postgres_update_query, record_to_update)


async def main():
    preload_checked_urls()
    await scrape(start_url, base_url, full_crawl=False)


start_time = time.time()
if __name__ == "__main__":
    asyncio.run(main())
