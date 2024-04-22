from scrape import scrape, url_exists, base_url

import asyncio

# Assuming url_exists and scrape are defined in your scrape.py
from scrape import url_exists, scrape


async def process_errors():
    with open("errors.txt", "r") as f:
        lines = f.readlines()

    tasks = []
    for line in lines:
        url = line.split("::")[0].strip()  # Remove newline characters
        if not url_exists(url):
            print("trying to scrape", url)
            # await scrape(url, base_url)  # Assuming the base_url is the same as the url
            tasks.append(scrape(url, base_url, "errors2.txt"))
            if len(tasks) == 15:
                print(f"Gathering tasks for {url}, count: {len(tasks)}")
                await asyncio.gather(*tasks)
                tasks = []
                await asyncio.sleep(0.5)

        if len(tasks) > 0 and len(tasks) < 15:
            print(f"Gathering tasks for {url}, count: {len(tasks)}")
            await asyncio.gather(*tasks)
            tasks = []
            await asyncio.sleep(0.5)


# Run the process_errors coroutine
asyncio.run(process_errors())
