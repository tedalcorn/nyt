#!/usr/bin/env python3
"""
Build data/presidents.json for the home-carousel headline chart.

Definition (deliberately simple): for each month, count the NYT articles whose
HEADLINE contains a president's surname as a whole word. That's it — no page,
section, or tag filters. This mirrors how the original hand-built file was made
("name in headline"), but now it regenerates from the live corpus every night,
so the chart always runs through the latest complete month.

Word-boundary matching (\bBush\b) avoids false positives like "ambush",
"Bushwick", or "trumpet". The current, in-progress calendar month is skipped so
a half-finished month never shows up as a misleading dip.

Output shape (consumed by renderCarouselPresidents() in index.html):
  {
    "months": ["2000-01", ..., "2026-06"],
    "Trump":  [int, ...],   # same length as months
    "Biden":  [...], "Obama": [...], "Bush": [...]
  }
"""
import json
import os
import re
import glob
from datetime import date
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")

# short label -> compiled whole-word headline matcher
PRESIDENTS = {
    "Trump": re.compile(r"\bTrump\b"),
    "Biden": re.compile(r"\bBiden\b"),
    "Obama": re.compile(r"\bObama\b"),
    "Bush":  re.compile(r"\bBush\b"),
}


def main():
    # counts[name][YYYY-MM] = int
    counts = {name: defaultdict(int) for name in PRESIDENTS}
    all_months = set()

    # Skip the current, in-progress calendar month (partial data -> misleading dip)
    current_month = date.today().strftime("%Y-%m")

    year_files = sorted(glob.glob(os.path.join(DATA_DIR, "articles_[0-9][0-9][0-9][0-9].json")))
    for path in year_files:
        year = os.path.basename(path)[len("articles_"):-len(".json")]
        with open(path) as f:
            articles = json.load(f)
        hits = 0
        for r in articles:
            m = r.get("m")
            if not m or m >= current_month:
                continue
            h = r.get("h") or ""
            all_months.add(m)
            for name, pat in PRESIDENTS.items():
                if pat.search(h):
                    counts[name][m] += 1
                    hits += 1
        print(f"  {year}: {len(articles):>6,} articles  ->  {hits:>5,} president-headline hits")

    months = sorted(all_months)
    out = {"months": months}
    for name in PRESIDENTS:
        out[name] = [counts[name].get(m, 0) for m in months]

    out_path = os.path.join(DATA_DIR, "presidents.json")
    with open(out_path, "w") as f:
        json.dump(out, f, separators=(",", ":"))

    print(f"\n  Wrote {out_path}")
    print(f"  {len(months)} months: {months[0]} -> {months[-1]}")
    for name in PRESIDENTS:
        print(f"    {name:6} total {sum(out[name]):>7,}   latest month {out[name][-1]}")


if __name__ == "__main__":
    main()
