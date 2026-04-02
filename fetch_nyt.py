"""
Fetch NYT Archive API data incrementally.

Downloads one JSON file per month (YYYY-MM.json) into data/raw/.
Only fetches months that haven't been downloaded yet.
Rate-limited to respect NYT's 10 req/min limit.
"""

import os
import json
import time
import requests
from datetime import datetime, date

API_KEY = os.environ.get("NYT_API_KEY")
if not API_KEY:
    raise RuntimeError("Set NYT_API_KEY environment variable")

BASE_URL = "https://api.nytimes.com/svc/archive/v1/{year}/{month}.json"
RAW_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "raw")
START_YEAR = 2000
START_MONTH = 1


def get_months_to_fetch():
    """Return list of (year, month) tuples that haven't been fetched yet."""
    os.makedirs(RAW_DIR, exist_ok=True)
    existing = set(f for f in os.listdir(RAW_DIR) if f.endswith(".json"))

    today = date.today()
    months = []
    year, month = START_YEAR, START_MONTH
    while (year, month) <= (today.year, today.month):
        filename = f"{year}-{month:02d}.json"
        if filename not in existing:
            months.append((year, month))
        year, month = (year + (month // 12), (month % 12) + 1)
    return months


def fetch_month(year, month, retries=5):
    """Fetch a single month from the Archive API with retry logic."""
    url = BASE_URL.format(year=year, month=month)
    params = {"api-key": API_KEY}

    for attempt in range(retries):
        resp = requests.get(url, params=params)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 429:
            wait = 2 ** (attempt + 1)
            print(f"  Rate limited, waiting {wait}s...")
            time.sleep(wait)
        else:
            print(f"  Error {resp.status_code}, retrying...")
            time.sleep(2)

    print(f"  Failed after {retries} attempts")
    return None


def main():
    months = get_months_to_fetch()
    if not months:
        print("All months already fetched.")
        return

    print(f"Fetching {len(months)} months from NYT Archive API...")

    for i, (year, month) in enumerate(months):
        print(f"  [{i+1}/{len(months)}] {year}-{month:02d}...", end=" ", flush=True)
        data = fetch_month(year, month)

        if data and "response" in data and "docs" in data["response"]:
            docs = data["response"]["docs"]
            filepath = os.path.join(RAW_DIR, f"{year}-{month:02d}.json")
            with open(filepath, "w") as f:
                json.dump(docs, f)
            print(f"{len(docs)} articles")
        else:
            print("no data")

        # Respect rate limit: 10 req/min = 6s between requests
        if i < len(months) - 1:
            time.sleep(6)

    print("Done.")


if __name__ == "__main__":
    main()
