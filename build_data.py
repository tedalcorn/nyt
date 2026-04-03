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
    # Strip multimedia credit prefixes from any name component
    # (e.g. firstname="Photographs", making fullname "Photographs George Etheredge")
    CREDIT_PREFIX = re.compile(
        r'^(Photographs?|Illustration|Illustrations|Drawing|Drawings|Map|Video|Graphic|Graphics|Photo)\s*',
        re.IGNORECASE
    )

    persons = byline.get("person", [])
    if persons:
        authors = []
        for p in persons:
            first = CREDIT_PREFIX.sub('', (p.get("firstname") or "").strip()).strip()
            middle = (p.get("middlename") or "").strip()
            last = (p.get("lastname") or "").strip()
            if not last:
                continue
            # Skip entries where the entire firstname was a media credit word
            # (e.g. firstname="Photographs", lastname="Smith") — these are photo
            # credits, not actual co-authors, and inflate multi-byline counts.
            if not first and not middle:
                continue
            # Normalize ALL CAPS last names
            if last.isupper():
                last = last.title()
            parts = [first, middle, last]
            fullname = " ".join(x for x in parts if x)
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
    # Multimedia credit prefixes to strip (e.g. "Photographs Leonard Greco")
    CREDIT_PREFIX = re.compile(
        r'^(Photographs?|Illustration|Illustrations|Drawing|Drawings|Map|Video|Graphic|Graphics|Photo)\s+',
        re.IGNORECASE
    )
    authors = []
    for name in names:
        name = name.strip()
        name = CREDIT_PREFIX.sub('', name).strip()
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
STATE_ALIASES = {
    "New York State": "New York",
    "Washington State": "Washington",
    "District of Columbia": "D.C.",
}
ABBREV_TO_STATE = {
    "Ala": "Alabama", "ALA": "Alabama",
    "Alaska": "Alaska", "ALASKA": "Alaska",
    "Ariz": "Arizona", "ARIZ": "Arizona", "AZ": "Arizona",
    "Ark": "Arkansas", "ARK": "Arkansas",
    "Calif": "California", "CALIF": "California", "California": "California",
    "Colo": "Colorado", "COLO": "Colorado", "Colorado": "Colorado",
    "Conn": "Connecticut",
    "Del": "Delaware",
    "DC": "D.C.", "Washington, DC": "D.C.",
    "Fla": "Florida", "FLA": "Florida", "Florida": "Florida",
    "Ga": "Georgia", "GA": "Georgia",
    "Hawaii": "Hawaii", "HAWAII": "Hawaii",
    "Idaho": "Idaho", "IDAHO": "Idaho",
    "Ill": "Illinois", "ILL": "Illinois",
    "Ind": "Indiana", "IND": "Indiana",
    "Iowa": "Iowa", "IOWA": "Iowa",
    "Kan": "Kansas", "KAN": "Kansas",
    "Ky": "Kentucky", "KY": "Kentucky",
    "La": "Louisiana", "LA": "Louisiana",
    "Me": "Maine",
    "Md": "Maryland", "MD": "Maryland", "Baltimore, Md": "Maryland",
    "Mass": "Massachusetts", "MASS": "Massachusetts",
    "Mich": "Michigan", "MICH": "Michigan",
    "Minn": "Minnesota", "MINN": "Minnesota", "Minnesota": "Minnesota",
    "Miss": "Mississippi", "MISS": "Mississippi",
    "Mo": "Missouri", "MO": "Missouri", "Missouri": "Missouri",
    "Mont": "Montana",
    "Neb": "Nebraska",
    "Nev": "Nevada", "NEV": "Nevada", "Nevada": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico", "New Mexico": "New Mexico",
    "NY": "New York", "NYC": "New York", "NYS Area": "New York",
    "Manhattan, NY": "New York", "Brooklyn, NY": "New York",
    "Queens, NY": "New York", "Bronx, NY": "New York",
    "Brooklyn-Queens, NY": "New York", "Newburgh, NY": "New York",
    "Niagara Falls, NY": "New York",
    "NC": "North Carolina", "North Carolina": "North Carolina",
    "ND": "North Dakota",
    "Ohio": "Ohio", "OHIO": "Ohio",
    "Okla": "Oklahoma", "OKLA": "Oklahoma",
    "Ore": "Oregon", "ORE": "Oregon", "Oregon": "Oregon",
    "Pa": "Pennsylvania", "PA": "Pennsylvania", "Penn": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "Tenn": "Tennessee", "TENN": "Tennessee",
    "Tex": "Texas", "TEX": "Texas",
    "Utah": "Utah",
    "Vt": "Vermont", "VT": "Vermont",
    "Va": "Virginia", "VA": "Virginia",
    "Wash": "Washington", "WASH": "Washington", "Wash.": "Washington",
    "W Va": "West Virginia", "W VA": "West Virginia", "WVa": "West Virginia",
    "Wis": "Wisconsin", "WIS": "Wisconsin",
    "Wyo": "Wyoming", "Wyoming": "Wyoming",
}


def glocation_to_state(loc):
    """Return canonical state name for a glocation string, or None."""
    if loc in US_STATES:
        return STATE_ALIASES.get(loc, loc)
    m = re.search(r'\(([^)]+)\)', loc)
    if m:
        return ABBREV_TO_STATE.get(m.group(1))
    return None


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

        # Skip records with implausible dates (API sometimes returns year 0001)
        if pub_date.year < 1990:
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

        # Exclude non-journalism content
        mat = doc.get("type_of_material", "") or ""
        if section == "Archives" or mat == "Paid Death Notice":
            skipped += 1
            continue

        # Merge renamed sections
        SECTION_MERGES = {
            "Fashion & Style": "Style",
            "Fashion": "Style",
            "Business Day": "Business",
            "Gameplay": "Crosswords & Games",
            "Book Review": "Books",
            "Guides": "Guide",
            "en Español": "En español",
            "Week in Review": "Sunday Review",
        }
        section = SECTION_MERGES.get(section, section)
        news_desk = doc.get("news_desk", "") or ""
        doc_type = doc.get("document_type", "") or ""
        web_url = doc.get("web_url", "") or ""

        # Print page info
        print_section = doc.get("print_section", "") or ""
        print_page = doc.get("print_page", "") or ""

        # Keywords (geographic, subject)
        # Note: keyword field names changed case in 2025
        # (glocations -> Location, subject -> Subject)
        subsection = doc.get("subsection_name", "") or ""
        glocations = []
        subjects = []
        for kw in (doc.get("keywords") or []):
            kw_name = kw.get("name", "")
            if kw_name in ("glocations", "Location"):
                glocations.append(kw["value"])
            elif kw_name in ("subject", "Subject"):
                subjects.append(kw["value"])

        # Canonical state names (for U.S. section articles — used by state detail panel)
        canonical_states = []
        if section == "U.S.":
            seen_states = set()
            for loc in glocations:
                st = glocation_to_state(loc)
                if st and st not in seen_states:
                    canonical_states.append(st)
                    seen_states.add(st)

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
            "subjects": subjects,
            "canonical_states": canonical_states,
        })

    print(f"  {len(articles):,} processed, {skipped} skipped")

    # Manual overrides for names the NYT API consistently truncates or misspells.
    # Key: wrong form as it appears in the API data. Value: correct full name.
    AUTHOR_OVERRIDES = {
        # "St." compound last names — API drops the second word of the last name.
        # Only add entries here when the correct full name is confirmed.
        "Nicholas St":  "Nicholas St. Fleur",
        # Other "St" truncations (Emily St, Brian St, Zach St, etc.) need
        # manual verification before adding — leave them for now.
        # Trailing "Photographs" suffix (byline parsed as "Name; Photographs by ...")
        "Ken Belson Photographs":   "Ken Belson",
        "Ilana Kaplan Photographs": "Ilana Kaplan",
        "Sarah Bahr Photographs":   "Sarah Bahr",
    }

    # Apply overrides to all articles so counts accumulate on the correct name
    for art in articles:
        art["authors"] = [AUTHOR_OVERRIDES.get(a, a) for a in art["authors"]]

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
    """Build per-author statistics with precise annual productivity."""
    from datetime import date as date_cls

    author_data = defaultdict(lambda: {
        "article_count": 0,
        "total_words": 0,
        "sections": Counter(),
        "years": set(),
        "first_date": None,
        "last_date": None,
        "annual_words": defaultdict(int),    # year -> words
        "annual_sections": defaultdict(Counter),  # year -> section counts
        "monthly_counts": defaultdict(int),  # YYYY-MM -> article count
    })

    for art in articles:
        n = art["n_authors"] or 1
        author_words = art["word_count"] // n if n > 0 else 0
        pub_date = art["pub_date"][:10]  # "YYYY-MM-DD"
        year = art["year"]
        for name in art["authors"]:
            d = author_data[name]
            d["article_count"] += 1
            d["total_words"] += author_words
            d["sections"][art["section"]] += 1
            d["years"].add(year)
            d["annual_words"][year] += author_words
            d["annual_sections"][year][art["section"]] += 1
            d["monthly_counts"][art["year_month"]] += 1
            if d["first_date"] is None or pub_date < d["first_date"]:
                d["first_date"] = pub_date
            if d["last_date"] is None or pub_date > d["last_date"]:
                d["last_date"] = pub_date

    authors = []
    for name, d in author_data.items():
        sections_ranked = d["sections"].most_common()
        primary_section = sections_ranked[0][0] if sections_ranked else ""
        secondary_section = sections_ranked[1][0] if len(sections_ranked) > 1 else ""
        years = sorted(d["years"])

        first_date = d["first_date"]  # "YYYY-MM-DD"
        last_date = d["last_date"]

        # --- Compute normalized annual words ---
        # For full interior years: use raw annual words directly.
        # For the first year: scale up by (365 / days_remaining_in_year_from_first_article).
        # For the last year: scale up by (365 / days_elapsed_in_year_to_last_article).
        # This annualizes partial years to a "words/year if they wrote at this rate all year" rate.
        # When first_year == last_year the whole period is one partial year; normalize over actual days span.
        annual_words_norm = {}
        if years and first_date and last_date:
            fd = date_cls.fromisoformat(first_date)
            ld = date_cls.fromisoformat(last_date)

            for y in years:
                raw = d["annual_words"][y]
                if len(years) == 1:
                    # Only one active year: normalize over actual span of activity
                    span_days = max((ld - fd).days + 1, 1)
                    annual_words_norm[y] = round(raw * 365 / span_days)
                elif y == years[0]:
                    # First year: days from first article to Dec 31
                    year_end = date_cls(y, 12, 31)
                    active_days = max((year_end - fd).days + 1, 1)
                    annual_words_norm[y] = round(raw * 365 / active_days)
                elif y == years[-1]:
                    # Last year: days from Jan 1 to last article
                    year_start = date_cls(y, 1, 1)
                    active_days = max((ld - year_start).days + 1, 1)
                    annual_words_norm[y] = round(raw * 365 / active_days)
                else:
                    # Interior full year: no normalization needed
                    annual_words_norm[y] = raw

        # avg_words_per_year: total words / actual date span in fractional years.
        # This avoids the distortion of averaging annualized edge years (which can
        # be wildly inflated when the first/last article falls in a short window).
        avg_words_per_year = 0
        if first_date and last_date and d["total_words"]:
            fd = date_cls.fromisoformat(first_date)
            ld = date_cls.fromisoformat(last_date)
            span_days = max((ld - fd).days, 1)
            # Don't compute for very short tenures — inflated by small denominator
            if span_days >= 90:
                span_years = span_days / 365.25
                avg_words_per_year = round(d["total_words"] / span_years)

        # all_sections: union of each year's primary section — lets an author
        # appear under multiple sections if their beat shifted over time.
        annual_primary = {
            y: ctr.most_common(1)[0][0]
            for y, ctr in d["annual_sections"].items() if ctr
        }
        all_sections = sorted(set(annual_primary.values()))
        if primary_section and primary_section not in all_sections:
            all_sections.insert(0, primary_section)

        authors.append({
            "name": name,
            "article_count": d["article_count"],
            "total_words": d["total_words"],
            "avg_words": round(d["total_words"] / d["article_count"]) if d["article_count"] else 0,
            "avg_words_per_year": avg_words_per_year,
            "primary_section": primary_section,
            "secondary_section": secondary_section,
            "all_sections": all_sections,
            "year_range": f"{years[0]}-{years[-1]}" if years else "",
            "first_year": years[0] if years else None,
            "last_year": years[-1] if years else None,
            "first_date": first_date,
            "last_date": last_date,
            "annual_words_norm": annual_words_norm,
            "monthly_counts": dict(d["monthly_counts"]),
            "beats": [],  # filled in later by build_beats()
        })

    authors.sort(key=lambda a: a["article_count"], reverse=True)
    print(f"  {len(authors):,} unique authors")
    return authors


_GENERIC_SUBJECTS = {
    'United States Politics and Government', 'Content Type: Personal Profile',
    'Content Type: Service', 'your-feed-science', 'your-feed-healthcare',
    'your-feed-internet', 'your-feed-animals', 'your-feed-weather',
    'States (US)', 'Research',
}
_GENERIC_PREFIXES = ('internal-', 'audio-', 'vis-', 'your-feed')
_INSTITUTIONAL_BYLINES = {
    'The New York Times', 'The Associated Press', 'The Editorial Board',
    'The Learning Network', 'New York Times Games', 'International Herald Tribune',
    'Reuters', 'The New York Times Books Staff', 'The Upshot Staff',
    'The Staff', 'The Times Insider Staff', 'The New York Times Sports Staff',
    'The New York Times Staff',
    'Bloomberg News', 'Associated Press', 'Bridge News', 'Field Level Media',
    'Der Spiegel', 'der Spiegel', 'The International Herald Tribune',
    'New York Times', 'New York Times Audio', 'New York Times Opinion',
    'The New York Times Opinion', 'The New York Times Magazine',
    'The Styles Desk', 'Retro Report', 'New York Times Cooking',
    'Insider Staff', 'the staff of The Morning',
    'Compiled by The New York Times',
}


def _is_generic_subject(s):
    if s in _GENERIC_SUBJECTS:
        return True
    return any(s.startswith(p) for p in _GENERIC_PREFIXES)


def build_beats(articles, authors_list):
    """Precompute beats data for instant Beats tab and author timelines.

    Returns (beats_json, author_beats_map) where:
      beats_json    — dict to write as beats.json
      author_beats_map — {name: [subject, ...]} top beats per author
    """
    import math

    author_section = {a['name']: a.get('primary_section', '') for a in authors_list}

    # Corpus subject frequency: docs per subject (deduplicated per article)
    corpus_freq = Counter()
    corpus_docs = len(articles)
    for art in articles:
        seen = set()
        for s in art.get('subjects', []):
            if not _is_generic_subject(s) and s not in seen:
                corpus_freq[s] += 1
                seen.add(s)

    # Group articles by author
    by_author = defaultdict(list)
    for art in articles:
        for name in art['authors']:
            by_author[name].append(art)

    subject_index = defaultdict(list)
    author_beats_map = {}

    for name, arts in by_author.items():
        if name in _INSTITUTIONAL_BYLINES:
            continue
        section = author_section.get(name, '')
        n = len(arts)
        freq = Counter()
        for art in arts:
            seen = set()
            for s in art.get('subjects', []):
                if not _is_generic_subject(s) and s not in seen:
                    freq[s] += 1
                    seen.add(s)

        # subject_index: require ≥3 articles on subject
        for subj, count in freq.items():
            if count >= 3:
                subject_index[subj].append({'name': name, 'count': count, 'total': n, 'section': section})

        # Per-author beats: same scoring as extractBeats() in index.html
        threshold = max(2, math.ceil(n * 0.03))
        scored = []
        for subj, count in freq.items():
            if count < threshold:
                continue
            corpus_count = corpus_freq.get(subj, 1)
            author_rate = count / n
            corpus_rate = corpus_count / corpus_docs
            ratio = author_rate / corpus_rate
            if ratio < 2:
                continue
            score = ratio * math.log(count + 1)
            scored.append((subj, score))
        scored.sort(key=lambda x: x[1], reverse=True)
        author_beats_map[name] = [s for s, _ in scored[:7]]

    # Sort each subject's reporters by count desc
    for subj in subject_index:
        subject_index[subj].sort(key=lambda x: x['count'], reverse=True)

    # Subject list sorted by corpus frequency
    subject_list = [
        {'subject': s, 'docCount': corpus_freq[s], 'reporters': len(subject_index[s])}
        for s in subject_index
    ]
    subject_list.sort(key=lambda x: x['docCount'], reverse=True)

    # Co-occurrences per subject: top 15 related subjects
    known = set(subject_index.keys())
    cooccur = defaultdict(Counter)
    for art in articles:
        subs = [s for s in art.get('subjects', []) if not _is_generic_subject(s) and s in known]
        for s1 in subs:
            for s2 in subs:
                if s2 != s1:
                    cooccur[s1][s2] += 1
    cooccur_top = {
        s: [[k, v] for k, v in c.most_common(15)]
        for s, c in cooccur.items() if s in known
    }

    beats_json = {
        'subjectList': subject_list,
        'subjectIndex': dict(subject_index),
        'corpusSubjectFreq': dict(corpus_freq),
        'corpusSubjectDocs': corpus_docs,
        'cooccur': cooccur_top,
    }
    return beats_json, author_beats_map


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

    # Words per section over time (all sections)
    top_sections = [s["name"] for s in sections if s["name"] not in ("", "(none)")]
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
    # Merge city-level tags into their parent country, and fix all-caps names.
    # Contested/ambiguous geographies (Gaza Strip, West Bank, Taiwan, Hong Kong, etc.) are left as-is.
    LOCATION_NORMALIZE = {
        "London (England)":      "Great Britain",
        "Paris (France)":        "France",
        "Beijing (China)":       "China",
        "Moscow (Russia)":       "Russia",
        "Berlin (Germany)":      "Germany",
        "Rome (Italy)":          "Italy",
        "Tokyo (Japan)":         "Japan",
        "Seoul (South Korea)":   "South Korea",
        "Baghdad (Iraq)":        "Iraq",
        "Cairo (Egypt)":         "Egypt",
        "Tehran (Iran)":         "Iran",
        "Kabul (Afghanistan)":   "Afghanistan",
        "Damascus (Syria)":      "Syria",
        "Kyiv (Ukraine)":        "Ukraine",
        "AFGHANISTAN":           "Afghanistan",
        "AFRICA":                "Africa",
    }

    world_articles = [a for a in articles if a["section"] == "World"]
    glocation_year = defaultdict(lambda: defaultdict(int))
    glocation_total = Counter()
    region_year = defaultdict(lambda: defaultdict(int))

    for art in world_articles:
        y = str(art["year"])
        for loc in art.get("glocations", []):
            loc = LOCATION_NORMALIZE.get(loc, loc)
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

    # --- US State coverage: use canonical_states pre-computed per article ---
    state_year = defaultdict(lambda: defaultdict(int))
    state_total = Counter()

    for art in articles:
        if art["section"] != "U.S.":
            continue
        y = str(art["year"])
        for state in art.get("canonical_states", []):
            state_year[state][y] += 1
            state_total[state] += 1

    top_states = [s for s, _ in state_total.most_common()]
    us_state_coverage = {
        "states": top_states,
        "state_trends": {s: dict(state_year[s]) for s in top_states},
        "years": all_years,
    }

    # Multi-byline trend by year (precomputed so Overview tab works without full articles array)
    multi_byline_by_year = defaultdict(lambda: {"total": 0, "multi": 0})
    for art in articles:
        y = str(art["year"])
        multi_byline_by_year[y]["total"] += 1
        if art["n_authors"] > 1:
            multi_byline_by_year[y]["multi"] += 1
    multi_byline_trend = [
        {
            "year": y,
            "total": d["total"],
            "multi": d["multi"],
            "pct": round(100 * d["multi"] / d["total"], 1) if d["total"] else 0,
        }
        for y, d in sorted(multi_byline_by_year.items())
    ]

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
        "multi_byline_trend": multi_byline_trend,
        "world_coverage": world_coverage,
        "us_state_coverage": us_state_coverage,
    }


def main():
    raw = load_all_articles()
    articles = process_articles(raw)
    authors = build_author_stats(articles)

    print("Building beats data...")
    beats_json, author_beats_map = build_beats(articles, authors)
    for a in authors:
        a['beats'] = author_beats_map.get(a['name'], [])
    print(f"  {len(beats_json['subjectList'])} unique beats subjects")

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
        if a.get("canonical_states"):
            rec["st"] = a["canonical_states"]  # canonical US state names
        if a.get("subsection"):
            rec["ss"] = a["subsection"]  # subsection
        if a.get("subjects"):
            rec["sb"] = a["subjects"]  # subject keywords
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

    with open(os.path.join(DATA_DIR, "beats.json"), "w") as f:
        json.dump(beats_json, f, separators=(',', ':'))
    print(f"Saved beats.json ({len(beats_json['subjectList'])} subjects)")


if __name__ == "__main__":
    main()
