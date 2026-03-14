"""
Build consolidated datasets from raw NYT Archive API data.

Reads data/raw/*.json, processes into:
  - data/articles.json  (all articles with extracted fields)
  - data/authors.json   (author stats)
  - data/dashboard.json (pre-computed stats for the dashboard)
"""

import os
import json
import re
from collections import defaultdict, Counter
from datetime import datetime

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(PROJECT_DIR, "data", "raw")
DATA_DIR = os.path.join(PROJECT_DIR, "data")


def load_all_articles():
    """Load and flatten all raw monthly JSON files."""
    articles = []
    files = sorted(f for f in os.listdir(RAW_DIR) if f.endswith(".json"))
    print(f"Loading {len(files)} monthly files...")

    for filename in files:
        with open(os.path.join(RAW_DIR, filename)) as f:
            docs = json.load(f)
        for doc in docs:
            articles.append(doc)

    print(f"  {len(articles):,} total raw articles")
    return articles


def extract_authors(byline):
    """Extract list of author dicts from a byline field."""
    if not byline or not isinstance(byline, dict):
        return []
    persons = byline.get("person", [])
    if persons:
        authors = []
        for p in persons:
            first = (p.get("firstname") or "").strip()
            middle = (p.get("middlename") or "").strip()
            last = (p.get("lastname") or "").strip()
            if not last:
                continue
            # Normalize ALL CAPS last names
            if last.isupper():
                last = last.title()
            parts = [first, middle, last]
            fullname = " ".join(p for p in parts if p)
            authors.append({
                "firstname": first,
                "middlename": middle,
                "lastname": last,
                "fullname": fullname,
            })
        return authors

    # Fallback: parse from "original" string (e.g. "By Sarah Mervosh and Mark Bonamo")
    original = (byline.get("original") or "").strip()
    if not original:
        return []
    # Strip leading "By " (case-insensitive)
    text = re.sub(r'^by\s+', '', original, flags=re.IGNORECASE)
    # Split on " and ", ", and ", ", "
    names = re.split(r',\s+and\s+|\s+and\s+|,\s+', text)
    authors = []
    for name in names:
        name = name.strip()
        if not name or len(name) < 3:
            continue
        parts = name.split()
        if len(parts) >= 2:
            first = parts[0]
            last = parts[-1]
            middle = " ".join(parts[1:-1]) if len(parts) > 2 else ""
            authors.append({
                "firstname": first,
                "middlename": middle,
                "lastname": last,
                "fullname": name,
            })
    return authors


def process_articles(raw_articles):
    """Process raw API articles into clean records."""
    articles = []
    skipped = 0

    for doc in raw_articles:
        pub_date_str = doc.get("pub_date", "")
        if not pub_date_str:
            skipped += 1
            continue

        # Parse date
        try:
            pub_date = datetime.fromisoformat(pub_date_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            skipped += 1
            continue

        # Skip anything before 2014
        if pub_date.year < 2014:
            skipped += 1
            continue

        headline = doc.get("headline", {})
        main_headline = headline.get("main", "") if isinstance(headline, dict) else ""

        authors = extract_authors(doc.get("byline"))
        word_count = doc.get("word_count") or 0
        try:
            word_count = int(word_count)
        except (ValueError, TypeError):
            word_count = 0

        section = doc.get("section_name", "") or ""
        news_desk = doc.get("news_desk", "") or ""
        doc_type = doc.get("document_type", "") or ""
        web_url = doc.get("web_url", "") or ""

        # Print page info
        print_section = doc.get("print_section", "") or ""
        print_page = doc.get("print_page", "") or ""

        # Geographic info (for World coverage analysis)
        subsection = doc.get("subsection_name", "") or ""
        glocations = []
        for kw in (doc.get("keywords") or []):
            if kw.get("name") == "glocations":
                glocations.append(kw["value"])

        articles.append({
            "pub_date": pub_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "year": pub_date.year,
            "month": pub_date.month,
            "year_month": pub_date.strftime("%Y-%m"),
            "headline": main_headline,
            "authors": [a["fullname"] for a in authors],
            "author_details": authors,
            "word_count": word_count,
            "section": section,
            "subsection": subsection,
            "news_desk": news_desk,
            "type": doc_type,
            "web_url": web_url,
            "print_section": print_section,
            "print_page": print_page,
            "n_authors": len(authors),
            "glocations": glocations,
        })

    print(f"  {len(articles):,} processed, {skipped} skipped")

    # Deduplicate author names: merge variants like "Jonah Engel Bromwich" / "Jonah E. Bromwich" / "Jonah Bromwich"
    # by mapping all to the most frequent version sharing the same first+last name
    print("  Deduplicating author names...")
    from collections import Counter as Ctr
    name_counts = Ctr()
    for art in articles:
        for name in art["authors"]:
            name_counts[name] += 1

    # Group by (first_name, last_name)
    groups = defaultdict(list)
    for name, count in name_counts.items():
        parts = name.split()
        if len(parts) >= 2:
            key = (parts[0].lower(), parts[-1].lower())
            groups[key].append((name, count))

    # Build canonical name map — only merge when names are compatible
    # (one is a subset of the other, or differs only by middle name abbreviation)
    canon_map = {}
    merges = 0

    def names_compatible(name_a, count_a, name_b, count_b):
        """Check if two names are variants of the same person."""
        pa, pb = name_a.split(), name_b.split()
        if len(pa) < 2 or len(pb) < 2:
            return False
        # Must share first and last name (case-insensitive)
        if pa[0].lower() != pb[0].lower() or pa[-1].lower() != pb[-1].lower():
            return False
        # Case-only difference (e.g., "DE" vs "de") — always merge
        if name_a.lower() == name_b.lower():
            return True
        ma = " ".join(pa[1:-1])  # middle parts of a
        mb = " ".join(pb[1:-1])  # middle parts of b
        # Both have different middle names/initials = different people
        if ma and mb:
            # If one middle is an abbreviation of the other (e.g., "E." matches "Engel")
            if len(ma) <= 2 and mb.lower().startswith(ma.rstrip('.').lower()):
                return True
            if len(mb) <= 2 and ma.lower().startswith(mb.rstrip('.').lower()):
                return True
            if ma.lower() == mb.lower():
                return True
            return False
        # One has a middle name, the other doesn't.
        # Only merge if the shorter-name variant has few articles (likely a typo/omission).
        # If both have significant counts, they're probably different people.
        shorter_count = count_b if not mb else count_a
        if shorter_count <= 10:
            return True
        return False

    for key, variants in groups.items():
        if len(variants) <= 1:
            continue
        # Sort by frequency (most common first)
        variants.sort(key=lambda x: x[1], reverse=True)
        # Try to merge each variant into the most frequent compatible one
        for i in range(1, len(variants)):
            name_i, count_i = variants[i]
            for j in range(i):
                name_j, count_j = variants[j]
                canonical_j = canon_map.get(name_j, name_j)
                canon_count = name_counts[canonical_j]
                if names_compatible(name_i, count_i, canonical_j, canon_count):
                    canon_map[name_i] = canonical_j
                    merges += 1
                    break

    # Save merge log for review
    merge_log = []
    for key, variants in sorted(groups.items()):
        if len(variants) <= 1:
            continue
        variants.sort(key=lambda x: x[1], reverse=True)
        canonical = variants[0]
        others = variants[1:]
        merge_log.append({
            "canonical": canonical[0],
            "canonical_count": canonical[1],
            "merged": [{"name": n, "count": c} for n, c in others],
        })

    merge_path = os.path.join(DATA_DIR, "name_merges.json")
    with open(merge_path, "w") as f:
        json.dump(merge_log, f, indent=2)
    print(f"  Merge log saved to {merge_path} ({len(merge_log)} groups)")

    # Apply to articles
    for art in articles:
        art["authors"] = [canon_map.get(n, n) for n in art["authors"]]

    print(f"  Merged {merges} name variants")
    return articles


def build_author_stats(articles):
    """Build per-author statistics."""
    author_data = defaultdict(lambda: {
        "article_count": 0,
        "total_words": 0,
        "sections": Counter(),
        "years": set(),
    })

    for art in articles:
        n = art["n_authors"] or 1
        author_words = art["word_count"] // n if n > 0 else 0
        for name in art["authors"]:
            d = author_data[name]
            d["article_count"] += 1
            d["total_words"] += author_words
            d["sections"][art["section"]] += 1
            d["years"].add(art["year"])

    authors = []
    for name, d in author_data.items():
        sections_ranked = d["sections"].most_common()
        primary_section = sections_ranked[0][0] if sections_ranked else ""
        secondary_section = sections_ranked[1][0] if len(sections_ranked) > 1 else ""
        years = sorted(d["years"])

        authors.append({
            "name": name,
            "article_count": d["article_count"],
            "total_words": d["total_words"],
            "avg_words": round(d["total_words"] / d["article_count"]) if d["article_count"] else 0,
            "primary_section": primary_section,
            "secondary_section": secondary_section,
            "year_range": f"{years[0]}-{years[-1]}" if years else "",
            "first_year": years[0] if years else None,
            "last_year": years[-1] if years else None,
        })

    authors.sort(key=lambda a: a["article_count"], reverse=True)
    print(f"  {len(authors):,} unique authors")
    return authors


def build_dashboard_data(articles, authors):
    """Pre-compute dashboard statistics."""
    # Articles per month
    monthly = Counter()
    monthly_words = defaultdict(int)
    for art in articles:
        ym = art["year_month"]
        monthly[ym] += 1
        monthly_words[ym] += art["word_count"]

    months_sorted = sorted(monthly.keys())
    articles_per_month = [{"month": m, "count": monthly[m], "words": monthly_words[m]} for m in months_sorted]

    # Section stats
    section_counts = Counter()
    section_words = defaultdict(int)
    for art in articles:
        s = art["section"] or "(none)"
        section_counts[s] += 1
        section_words[s] += art["word_count"]

    sections = []
    for s, count in section_counts.most_common():
        sections.append({
            "name": s,
            "count": count,
            "total_words": section_words[s],
            "avg_words": round(section_words[s] / count) if count else 0,
        })

    # Words per section over time (for top sections)
    top_sections = [s["name"] for s in sections[:20] if s["name"] not in ("", "(none)")]
    section_time = defaultdict(lambda: defaultdict(lambda: {"count": 0, "words": 0}))
    for art in articles:
        s = art["section"]
        if s in top_sections:
            y = str(art["year"])
            section_time[s][y]["count"] += 1
            section_time[s][y]["words"] += art["word_count"]

    section_trends = {}
    all_years = sorted(set(str(a["year"]) for a in articles))
    for s in top_sections:
        trend = []
        for y in all_years:
            d = section_time[s][y]
            avg = round(d["words"] / d["count"]) if d["count"] else 0
            trend.append({"year": y, "count": d["count"], "avg_words": avg})
        section_trends[s] = trend

    # Top 25 authors (by article count, 25+ articles)
    top_authors = [a for a in authors if a["article_count"] >= 25][:50]

    # Top 25 wordiest (30+ articles, excluding Opinion/Magazine)
    excluded = {"Opinion", "Magazine", "T Magazine"}
    wordiest = [a for a in authors
                if a["article_count"] >= 30 and a["primary_section"] not in excluded]
    wordiest.sort(key=lambda a: a["avg_words"], reverse=True)
    wordiest = wordiest[:25]

    # --- World coverage: glocations by year ---
    world_articles = [a for a in articles if a["section"] == "World"]
    glocation_year = defaultdict(lambda: defaultdict(int))
    glocation_total = Counter()
    region_year = defaultdict(lambda: defaultdict(int))

    for art in world_articles:
        y = str(art["year"])
        for loc in art.get("glocations", []):
            glocation_year[loc][y] += 1
            glocation_total[loc] += 1
        sub = art.get("subsection", "")
        if sub:
            region_year[sub][y] += 1

    # Top 40 locations
    top_locations = [loc for loc, _ in glocation_total.most_common(40)]
    world_coverage = {
        "locations": top_locations,
        "location_trends": {loc: dict(glocation_year[loc]) for loc in top_locations},
        "region_trends": {r: dict(region_year[r]) for r in sorted(region_year.keys())},
        "years": all_years,
    }

    # --- US State coverage: glocations from U.S. section ---
    US_STATES = {
        "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
        "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
        "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana",
        "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
        "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
        "New Hampshire", "New Jersey", "New Mexico", "New York State",
        "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
        "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
        "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington State",
        "West Virginia", "Wisconsin", "Wyoming",
        "District of Columbia",
    }
    # Map NYT glocation names to canonical state names
    STATE_ALIASES = {
        "New York State": "New York",
        "Washington State": "Washington",
        "District of Columbia": "D.C.",
    }

    us_articles = [a for a in articles if a["section"] == "U.S."]
    state_year = defaultdict(lambda: defaultdict(int))
    state_total = Counter()

    for art in us_articles:
        y = str(art["year"])
        for loc in art.get("glocations", []):
            if loc in US_STATES:
                canonical = STATE_ALIASES.get(loc, loc)
                state_year[canonical][y] += 1
                state_total[canonical] += 1

    top_states = [s for s, _ in state_total.most_common()]
    us_state_coverage = {
        "states": top_states,
        "state_trends": {s: dict(state_year[s]) for s in top_states},
        "years": all_years,
    }

    # Summary stats
    total_words = sum(a["word_count"] for a in articles)
    total_articles = len(articles)
    total_authors = len(authors)
    authors_25plus = len([a for a in authors if a["article_count"] >= 25])
    unique_sections = len(set(a["section"] for a in articles if a["section"]))
    date_range = f"{months_sorted[0]} to {months_sorted[-1]}" if months_sorted else ""

    return {
        "summary": {
            "total_articles": total_articles,
            "total_words": total_words,
            "total_authors": total_authors,
            "authors_25plus": authors_25plus,
            "unique_sections": unique_sections,
            "date_range": date_range,
            "first_month": months_sorted[0] if months_sorted else "",
            "last_month": months_sorted[-1] if months_sorted else "",
        },
        "articles_per_month": articles_per_month,
        "sections": sections,
        "section_trends": section_trends,
        "all_years": all_years,
        "top_authors": top_authors,
        "wordiest_authors": wordiest,
        "world_coverage": world_coverage,
        "us_state_coverage": us_state_coverage,
    }


def main():
    raw = load_all_articles()
    articles = process_articles(raw)
    authors = build_author_stats(articles)
    dashboard = build_dashboard_data(articles, authors)

    os.makedirs(DATA_DIR, exist_ok=True)

    # Save articles split by year (keeps files under GitHub's 100MB limit)
    # Strip URL prefix to save space
    URL_PREFIX = "https://www.nytimes.com"
    by_year = defaultdict(list)
    for a in articles:
        url = a["web_url"]
        if url.startswith(URL_PREFIX):
            url = url[len(URL_PREFIX):]
        rec = {
            "h": a["headline"],        # headline
            "a": a["authors"],         # authors
            "s": a["section"],         # section
            "d": a["pub_date"][:10],   # date
            "m": a["year_month"],      # year-month
            "w": a["word_count"],      # word count
            "u": url,                  # url (path only)
            "ps": a["print_section"],  # print section
            "pp": a["print_page"],     # print page
        }
        if a.get("glocations"):
            rec["g"] = a["glocations"]  # glocations
        if a.get("subsection"):
            rec["ss"] = a["subsection"]  # subsection
        by_year[a["year"]].append(rec)

    years = sorted(by_year.keys())
    for year in years:
        filepath = os.path.join(DATA_DIR, f"articles_{year}.json")
        with open(filepath, "w") as f:
            json.dump(by_year[year], f, separators=(',', ':'))
    print(f"Saved article files for {len(years)} years ({sum(len(v) for v in by_year.values()):,} articles)")

    with open(os.path.join(DATA_DIR, "authors.json"), "w") as f:
        json.dump(authors, f)
    print(f"Saved authors.json ({len(authors):,} authors)")

    with open(os.path.join(DATA_DIR, "dashboard.json"), "w") as f:
        json.dump(dashboard, f)
    print(f"Saved dashboard.json")


if __name__ == "__main__":
    main()
