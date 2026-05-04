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
import html as html_mod
from collections import defaultdict, Counter
import math
from datetime import datetime, date

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_DIR, "data", "raw")
DATA_DIR = os.path.join(PROJECT_DIR, "data")

# Author name overrides: wrong API form → canonical form.
# See data/author_overrides_notes.md for rationale and negative assertions.
with open(os.path.join(DATA_DIR, "author_overrides.json"), encoding="utf-8") as _f:
    AUTHOR_OVERRIDES = json.load(_f)


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
        r'^(Photographs?|Illustration|Illustrations|Drawing|Drawings|Map|Video|Graphic|Graphics|Photo'
        r'|Interviews?|Review)\s*',
        re.IGNORECASE
    )

    # Trailing collaboration words that sometimes bleed into byline name fields
    TRAILING_WORDS = re.compile(
        r'\s+(With|Compiled|Reporting|Contributing)$', re.IGNORECASE
    )

    # Detect API mis-parse of "Reported by X" → {firstname:"Reported", middlename:"X", lastname:...}
    # These articles have the credit word as firstname, losing the actual last name.
    # Skip the person array entirely and fall through to the original string.
    FIRSTNAME_CREDIT = re.compile(r'^(Reported|Reporting)$', re.IGNORECASE)

    persons = byline.get("person", [])
    if persons and any(FIRSTNAME_CREDIT.match((p.get("firstname") or "").strip()) for p in persons):
        persons = []  # malformed — force original string fallback
    # Detect "By X, As Told To Y" bylines — the API person array gives malformed entries
    # like {firstname:"As", lastname:"To"} — force original string fallback instead
    if persons and 'as told to' in (byline.get("original") or "").lower():
        persons = []  # malformed — force original string fallback

    def _clean(s):
        """Decode HTML entities and normalize non-breaking spaces in a name component."""
        return html_mod.unescape((s or "")).replace('\xa0', ' ').replace('\u00a0', ' ').strip()

    if persons:
        authors = []
        for p in persons:
            first = CREDIT_PREFIX.sub('', _clean(p.get("firstname"))).strip()
            middle = _clean(p.get("middlename"))
            last = _clean(p.get("lastname"))
            # API sometimes stores literal "None" string for null values (2008-2014 era)
            if first.lower() == 'none':
                first = ''
            if last.lower() in ('', 'none'):
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
            fullname = TRAILING_WORDS.sub('', fullname).strip()
            # Strip API parenthetical suffixes like "(NYT COMPILED BY ...)" that
            # create spurious name variants (e.g. "Jennifer 8. Lee (NYT COMPILED BY ...)")
            fullname = re.sub(r'\s*\(NYT[^)]*\)', '', fullname).strip()
            # Normalize compact double-initials: "A.o. Scott" → "A. O. Scott"
            fullname = re.sub(
                r'([A-Za-z])\.([A-Za-z])\.',
                lambda m: m.group(1).upper() + '. ' + m.group(2).upper() + '.',
                fullname
            )
            fullname = ' '.join(fullname.split())  # normalize any extra whitespace
            authors.append({
                "firstname": first,
                "middlename": middle,
                "lastname": last,
                "fullname": fullname,
            })
        if authors:
            return authors
        # All persons had empty lastnames — fall through to original string fallback

    # Fallback: parse from "original" string (e.g. "By Sarah Mervosh and Mark Bonamo")
    original = _clean(byline.get("original"))
    if not original:
        return []
    # Strip leading "By " (case-insensitive)
    text = re.sub(r'^by\s+', '', original, flags=re.IGNORECASE)
    # "As told to NAME" / "As told to NAME and NAME" — credit the interviewer/writer
    text = re.sub(r'^as\s+told\s+to\s+', '', text, flags=re.IGNORECASE)
    # Handle "Author: Written With Other" — colon separates main author from contribution note.
    # Truncate at colon so "Malia Mills: Written With Alex Kuczynski" → "Malia Mills".
    if ':' in text:
        text = text.split(':')[0].strip()
    # Strip sentence-style attributions before trying to parse names
    text = re.sub(
        r'^(This (?:article|story) was (?:reported(?: and written)?|written and reported|compiled) by'
        r'|The following article (?:is based on reporting|was reported) by'
        r'|Reporting by|Reported by)\s+',
        '', text, flags=re.IGNORECASE
    )
    # Split on " and ", ", and ", ", "
    names = re.split(r',\s+and\s+|\s+and\s+|,\s+', text)
    # Multimedia/format credit prefixes to strip (e.g. "Photographs Leonard Greco", "Interview Jim Rutenberg")
    CREDIT_PREFIX = re.compile(
        r'^(Photographs?|Illustration|Illustrations|Drawing|Drawings|Map|Video|Graphic|Graphics|Photo'
        r'|Interviews?|Review|Reported\s+by|Reported|Reporting\s+by|Reporting)\s+',
        re.IGNORECASE
    )
    # Institutional / wire-service strings that are not person names
    NON_PERSON = re.compile(
        r'^(wire reports?|from wire reports?|from news reports?|from staff reports?'
        r'|news reports?|staff reports?|reporting by the new york times'
        r'|reported by the new york times)\s*$',
        re.IGNORECASE
    )
    authors = []
    for name in names:
        name = name.strip()
        name = CREDIT_PREFIX.sub('', name).strip()
        if not name or len(name) < 3 or len(name) > 80:
            continue
        # Skip junk entries from malformed "original" byline strings
        if name[0] in '!-(\'&<‘’“”':
            continue
        # Strip leading possessive artifact — "s Dave Itzkoff" (bare s prefix from
        # a mis-split "’s" possessive in the original byline string).
        name = re.sub(r'^s\s+(?=[A-Z])', '', name).strip()  # strip bare-s prefix artifacts
        if not name or len(name) < 3:
            continue
        if '<' in name or '&#' in name or '|' in name:
            continue
        if name.lower().startswith('compiled by') or name.lower().startswith('special to'):
            continue
        if name.lower().startswith('written by') or name.lower().startswith('written and reported by'):
            continue
        if name.lower().startswith('interviews ') or name.lower().startswith('interviews:'):
            continue
        # Skip wire/institutional noise and contribution-credit fragments
        if NON_PERSON.match(name):
            continue
        if re.search(r'\bcontributed\b', name, re.IGNORECASE):
            continue
        # Fragment names like "and Y NEWMAN" are truncated "ANDY NEWMAN" — the API split
        # on " and " inside the name (e.g. "ANDY" → "A" + "NY"). Reconstruct by prepending
        # "And" to recover the original: "AndY NEWMAN".title() → "Andy Newman".
        m = re.match(r'^and\s+([A-Z].*)', name)
        if m:
            name = ('And' + m.group(1)).title()
        name = TRAILING_WORDS.sub('', name).strip()
        # Strip API parenthetical suffixes like "(NYT)" or "(NYT COMPILED BY ...)"
        name = re.sub(r'\s*\(NYT[^)]*\)', '', name).strip()
        # Normalize compact double-initials: "A.o. Scott" → "A. O. Scott"
        name = re.sub(
            r'([A-Za-z])\.([A-Za-z])\.',
            lambda m2: m2.group(1).upper() + '. ' + m2.group(2).upper() + '.',
            name
        )
        name = ' '.join(name.split())  # normalize whitespace
        # Skip API null artifacts stored as string "None"
        if all(p.lower() == 'none' for p in name.split()):
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
    "N.Y": "New York", "N.Y.": "New York",
    "Manhattan, NY": "New York", "Brooklyn, NY": "New York",
    "Queens, NY": "New York", "Bronx, NY": "New York",
    "Brooklyn-Queens, NY": "New York", "Newburgh, NY": "New York",
    "Niagara Falls, NY": "New York",
    "N.J": "New Jersey", "N.J.": "New Jersey",
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


_NYC_LOCS = {
    "New York City", "Manhattan", "Brooklyn", "Queens", "The Bronx", "Bronx",
    "Staten Island", "Harlem", "Manhattan (NYC)", "New York City (NYC)",
}

def glocation_to_state(loc):
    """Return canonical state name for a glocation string, or None."""
    if loc in _NYC_LOCS:
        return "New York"
    if loc in US_STATES:
        return STATE_ALIASES.get(loc, loc)
    if loc == "Washington (State)":
        return "Washington"
    m = re.search(r'\(([^)]+)\)', loc)
    if m:
        return ABBREV_TO_STATE.get(m.group(1))
    return None


SECTION_MERGES = {
    "Fashion & Style": "Style",
    "Fashion": "Style",
    "Business Day": "Business",
    "Gameplay": "Crosswords & Games",
    "Book Review": "Books",
    "Guides": "Guide",
    "en Español": "En español",
    "Week in Review": "Sunday Review",
    # Defunct/absorbed sections merged into closest surviving equivalent
    "Great Homes & Destinations": "Real Estate",  # luxury RE supplement 2002-2014
    "At Home": "Style",                           # COVID-era home-life section 2020-2022
    "Critic's Choice": "Arts",                    # arts picks feature folded into Arts
}


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
        print_headline = (headline.get("print_headline", "") or "") if isinstance(headline, dict) else ""
        kicker = (headline.get("kicker", "") or "") if isinstance(headline, dict) else ""

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
        # Exclude /college/ syndication URLs (Times Wire Service re-publications
        # to student newspapers — indexed as separate articles but duplicate content)
        web_url_raw = doc.get("web_url", "") or ""
        if "/college/" in web_url_raw:
            skipped += 1
            continue

        # Merge renamed sections
        section = SECTION_MERGES.get(section, section)

        # "Quote of the Day" column is filed under section_name='Corrections' by the API,
        # but it's a distinct editorial feature. Reassign to "Today's Paper" so it doesn't
        # inflate the Corrections section count or appear in corrections analysis.
        headline_main = (doc.get("headline") or {}).get("main", "") or ""
        if section == "Corrections" and (mat == "Quote" or headline_main.startswith("Quote of the Day")):
            section = "Today's Paper"

        # Override section for obituaries filed under subject sections.
        # 2001-2010: tom was usually "Obituary; Biography" (or "Obituary"/
        # "Biography; Obituary") but section was Arts/Sports/Business/etc.
        # 2011-2015: tom was "Obituary (Obit)" but again section was a subject.
        # Accept all four obit-tom variants and the Obits desk so the Obituaries
        # section trend reflects all obituary articles, not just the few
        # tagged section_name='Obituaries'.
        if (mat in {"Obituary (Obit)", "Obituary", "Obituary; Biography", "Biography; Obituary"}
                and section != "Obituaries"):
            section = "Obituaries"
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
        persons_kw = []
        organizations_kw = []
        for kw in (doc.get("keywords") or []):
            kw_name = kw.get("name", "")
            if kw_name in ("glocations", "Location"):
                glocations.append(kw["value"])
            elif kw_name in ("subject", "Subject"):
                # Apply continuity merges at ingestion so beats + compact format stay in sync
                subjects.append(_normalize_subject_kw(SUBJECT_RENAMES.get(kw["value"], kw["value"])))
            elif kw_name in ("persons", "Persons"):
                persons_kw.append(kw["value"])
            elif kw_name in ("organizations", "Organizations"):
                organizations_kw.append(_normalize_org_kw(kw["value"]))

        # Canonical state names — computed for both "U.S." and "New York" sections
        canonical_states = []
        if section in ("U.S.", "New York"):
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
            "print_headline": print_headline,
            "kicker": kicker,
            "authors": [a["fullname"] for a in authors],
            "author_details": authors,
            "word_count": word_count,
            "section": section,
            "subsection": subsection,
            "news_desk": news_desk,
            "type": doc_type,
            "type_of_material": mat,
            "web_url": web_url,
            "print_section": print_section,
            "print_page": print_page,
            "n_authors": len(authors),
            "glocations": glocations,
            "subjects": subjects,
            "persons": persons_kw,
            "organizations": organizations_kw,
            "canonical_states": canonical_states,
        })

    print(f"  {len(articles):,} processed, {skipped} skipped")

    # Author name overrides loaded from data/author_overrides.json at module level.
    # See data/author_overrides_notes.md for rationale and negative assertions.

    # Apply overrides iteratively (handles chains like "X Nyt" → "X" → "X Y. X")
    for _ in range(3):
        changed = False
        for art in articles:
            new = [AUTHOR_OVERRIDES.get(a, a) for a in art["authors"]]
            if new != art["authors"]:
                art["authors"] = new
                changed = True
        if not changed:
            break

    # Deduplicate author names: merge variants like "Jonah Engel Bromwich" / "Jonah E. Bromwich" / "Jonah Bromwich"
    # by mapping all to the most frequent version sharing the same first+last name
    print("  Deduplicating author names...")
    name_counts = Counter()
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
        "annual_blog_counts": defaultdict(int),  # year -> blog article count
        "annual_blog_words": defaultdict(int),   # year -> words from blog articles
        "annual_podcast_counts": defaultdict(int),  # year -> podcast article count
        "annual_podcast_words": defaultdict(int),   # year -> words from podcast articles
        "annual_live_counts": defaultdict(int),   # year -> /live/ and Brief article count
        "annual_live_words": defaultdict(int),    # year -> words from /live/ and Brief articles
        "shared_byline_count": 0,
        "monthly_shared_counts": defaultdict(int),  # YYYY-MM -> shared article count
        "coauthors": Counter(),
        "zero_word_articles": 0,
        "solo_text_articles": 0,  # solo bylines with word_count > 200
        "wc_hist": [0] * 21,     # word-count histogram: 21 bins of 200 words (last = 4000+)
    })

    for art in articles:
        # Only count human (non-institutional) authors for shared-byline purposes
        human_authors = [a for a in art["authors"] if a not in _INSTITUTIONAL_BYLINES]
        n = len(human_authors) or 1
        author_words = art["word_count"] // n if n > 0 else 0
        pub_date = art["pub_date"][:10]  # "YYYY-MM-DD"
        year = art["year"]
        is_shared = len(human_authors) > 1
        for name in art["authors"]:
            d = author_data[name]
            d["article_count"] += 1
            d["total_words"] += author_words
            d["sections"][art["section"]] += 1
            d["years"].add(year)
            d["annual_words"][year] += author_words
            d["annual_sections"][year][art["section"]] += 1
            d["monthly_counts"][art["year_month"]] += 1
            if is_blog_url(art.get("web_url", "")):
                d["annual_blog_counts"][year] += 1
                d["annual_blog_words"][year] += author_words
            if is_live_url(art.get("web_url", "")) or art.get("type_of_material") == "Brief":
                d["annual_live_counts"][year] += 1
                d["annual_live_words"][year] += author_words
            if is_podcast_article(art.get("section"), art.get("web_url", ""), art.get("kicker", "")):
                d["annual_podcast_counts"][year] += 1
                d["annual_podcast_words"][year] += author_words
            if art["word_count"] == 0:
                d["zero_word_articles"] += 1
            else:
                bin_idx = min(art["word_count"] // 200, 20)
                d["wc_hist"][bin_idx] += 1
            if not is_shared and art["word_count"] > 200:
                d["solo_text_articles"] += 1
            if is_shared:
                d["shared_byline_count"] += 1
                d["monthly_shared_counts"][art["year_month"]] += 1
                for coname in human_authors:
                    if coname != name:
                        d["coauthors"][coname] += 1
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
            fd = date.fromisoformat(first_date)
            ld = date.fromisoformat(last_date)

            for y in years:
                raw = d["annual_words"][y]
                if len(years) == 1:
                    # Only one active year: normalize over actual span of activity
                    span_days = max((ld - fd).days + 1, 1)
                    annual_words_norm[y] = round(raw * 365 / span_days)
                elif y == years[0]:
                    # First year: days from first article to Dec 31
                    year_end = date(y, 12, 31)
                    active_days = max((year_end - fd).days + 1, 1)
                    annual_words_norm[y] = round(raw * 365 / active_days)
                elif y == years[-1]:
                    # Last year: days from Jan 1 to last article
                    year_start = date(y, 1, 1)
                    active_days = max((ld - year_start).days + 1, 1)
                    annual_words_norm[y] = round(raw * 365 / active_days)
                else:
                    # Interior full year: no normalization needed
                    annual_words_norm[y] = raw

        # Normalize blog words using same scaling factors as annual_words_norm
        annual_blog_words_norm = {}
        for y in years:
            raw_total = d["annual_words"].get(y, 0)
            raw_blog = d["annual_blog_words"].get(y, 0)
            if raw_total > 0 and raw_blog > 0 and y in annual_words_norm:
                annual_blog_words_norm[y] = round(annual_words_norm[y] * raw_blog / raw_total)

        # Same normalization for podcast words
        annual_podcast_words_norm = {}
        for y in years:
            raw_total = d["annual_words"].get(y, 0)
            raw_pod = d["annual_podcast_words"].get(y, 0)
            if raw_total > 0 and raw_pod > 0 and y in annual_words_norm:
                annual_podcast_words_norm[y] = round(annual_words_norm[y] * raw_pod / raw_total)

        # Same normalization for live blog words
        annual_live_words_norm = {}
        for y in years:
            raw_total = d["annual_words"].get(y, 0)
            raw_live = d["annual_live_words"].get(y, 0)
            if raw_total > 0 and raw_live > 0 and y in annual_words_norm:
                annual_live_words_norm[y] = round(annual_words_norm[y] * raw_live / raw_total)

        # avg_words_per_year: total words / actual date span in fractional years.
        # This avoids the distortion of averaging annualized edge years (which can
        # be wildly inflated when the first/last article falls in a short window).
        avg_words_per_year = 0
        if first_date and last_date and d["total_words"]:
            fd = date.fromisoformat(first_date)
            ld = date.fromisoformat(last_date)
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
        # Sort sections by total article count desc, limit to 6 most frequent
        sec_counts = {s: d["sections"][s] for s in set(annual_primary.values())}
        if primary_section and primary_section not in sec_counts:
            sec_counts[primary_section] = d["sections"].get(primary_section, 0)
        all_sections = [s for s, _ in sorted(sec_counts.items(), key=lambda x: -x[1])][:6]

        article_count = d["article_count"]
        shared_count = d["shared_byline_count"]
        zero_word_rate = d["zero_word_articles"] / article_count if article_count else 0
        shared_rate = shared_count / article_count if article_count else 0
        # Exclude zero-word articles, /live/ entries, Brief items, AND podcasts from avg —
        # all these are non-standard formats that don't reflect a reporter's article writing.
        live_count = sum(d["annual_live_counts"].values())
        live_words = sum(d["annual_live_words"].values())
        pod_count = sum(d["annual_podcast_counts"].values())
        pod_words = sum(d["annual_podcast_words"].values())
        nonzero_count = article_count - d["zero_word_articles"] - live_count - pod_count
        nonzero_words = d["total_words"] - live_words - pod_words
        avg_words = round(nonzero_words / nonzero_count) if nonzero_count else 0
        # Likely non-editorial / collaborative byline: photographers, video producers,
        # podcast staff, crossword constructors, illustrators, etc.
        # Five routes to flagging:
        #   1. Photo/video: high shared rate + many zero-word articles
        #   2. Pure zero-word: virtually all articles have zero words (video/interactive producers
        #      who don't share bylines frequently but still produce no text content)
        #   3. Low-word shared: nearly always shared + very low avg words (illustrators,
        #      photographers whose articles have captions but no bylined text)
        #   4. Podcast / audio: primary section is Podcasts
        #   5. Other structural: section is Crosswords & Games or Briefing + very high shared
        is_photo_video = (
            article_count >= 5 and
            shared_rate >= 0.75 and
            zero_word_rate >= 0.35
        )
        is_pure_zero_word = (
            article_count >= 20 and
            zero_word_rate >= 0.95
        )
        is_low_word_shared = (
            article_count >= 5 and
            shared_rate >= 0.90 and
            avg_words < 100
        )
        is_podcast = (
            article_count >= 5 and
            primary_section == "Podcasts"
        )
        is_structural = (
            article_count >= 5 and
            shared_rate >= 0.90 and
            primary_section in ("Crosswords & Games", "Briefing")
        )
        # Carve-out: anyone with 20+ solo text articles (solo byline, >200 words) has done
        # real reporting and should NOT be excluded — catches reporters who later transitioned
        # to podcasts/video (e.g. Michael Barbaro) or visual journalists who occasionally wrote.
        has_reporting_history = d["solo_text_articles"] >= 20
        likely_multimedia = (is_photo_video or is_pure_zero_word or is_low_word_shared or is_podcast or is_structural) and not has_reporting_history
        top_coauthors = dict(d["coauthors"].most_common(10))

        authors.append({
            "name": name,
            "article_count": article_count,
            "total_words": d["total_words"],
            "avg_words": avg_words,  # uses nonzero_words / nonzero_count (excludes blogs/live/briefs/podcasts)
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
            "annual_words": dict(d["annual_words"]),
            "monthly_counts": dict(d["monthly_counts"]),
            "annual_blog_counts": dict(d["annual_blog_counts"]) if any(d["annual_blog_counts"].values()) else {},
            "annual_blog_words_norm": annual_blog_words_norm if annual_blog_words_norm else {},
            "annual_podcast_counts": dict(d["annual_podcast_counts"]) if any(d["annual_podcast_counts"].values()) else {},
            "annual_podcast_words_norm": annual_podcast_words_norm if annual_podcast_words_norm else {},
            "annual_live_counts": dict(d["annual_live_counts"]) if any(d["annual_live_counts"].values()) else {},
            "annual_live_words_norm": annual_live_words_norm if annual_live_words_norm else {},
            "shared_byline_count": shared_count,
            "monthly_shared_counts": dict(d["monthly_shared_counts"]),
            "coauthors": top_coauthors,
            "likely_multimedia": likely_multimedia,
            "solo_text_articles": d["solo_text_articles"],
            "wc_hist": d["wc_hist"],  # 21-bin word-count histogram (200-word bins, last = 4000+)
            "beats": [],  # filled in later by build_beats()
        })

    # Filter coauthors to only include authors exported to authors.json (>= 2 articles)
    # so that collaborator links in the UI always resolve to a valid profile
    valid_coauthor_names = {name for name, d in author_data.items() if d["article_count"] >= 2}
    for a in authors:
        a["coauthors"] = {k: v for k, v in a["coauthors"].items() if k in valid_coauthor_names}

    authors.sort(key=lambda a: a["article_count"], reverse=True)
    print(f"  {len(authors):,} unique authors")
    return authors


# Subject keyword renames: the NYT changed tag names over time; these map old → new
# so that beats data and author profiles show a continuous timeline.
# Verified by checking that old tag drops to ~0 exactly when new tag appears.
SUBJECT_RENAMES = {
    # Video/audio recordings
    "Recordings and Downloads (Video)":     "Video Recordings and Downloads",
    "RECORDINGS (VIDEO)":                   "Video Recordings and Downloads",
    "Recordings and Downloads (Audio)":     "Audio Recordings, Downloads and Streaming",
    "RECORDINGS (AUDIO)":                   "Audio Recordings, Downloads and Streaming",
    "DVD (DIGITAL VERSATILE DISK)":         "DVD (Digital Versatile Disc)",
    # Defense / military
    "ARMAMENT, DEFENSE AND MILITARY FORCES": "Defense and Military Forces",
    "UNITED STATES ARMAMENT AND DEFENSE":   "United States Defense and Military Forces",
    # Law enforcement / justice
    "Police Brutality and Misconduct":      "Police Brutality, Misconduct and Shootings",
    "Suits and Litigation":                 "Suits and Litigation (Civil)",
    "AMNESTIES AND PARDONS":                "Amnesties, Commutations and Pardons",
    "ANTITRUST ACTIONS AND LAWS":           "Antitrust Laws and Competition Issues",
    # Politics / society
    "Demonstrations and Riots":             "Demonstrations, Protests and Riots",
    "IMMIGRATION AND REFUGEES":             "Immigration and Emigration",
    "PUBLIC OPINION":                       "Polls and Public Opinion",
    "Children and Youth":                   "Children and Childhood",
    "Intelligence Services":               "Espionage and Intelligence Services",
    # Business / economy
    "RETAIL STORES AND TRADE":              "Shopping and Retail",
    "FACTORIES AND INDUSTRIAL PLANTS":      "Factories and Manufacturing",
    "HOTELS AND MOTELS":                    "Hotels and Travel Lodgings",
    "NIGHTCLUBS AND CABARETS":              "Bars and Nightclubs",
    "Fringe Benefits":                      "Employee Fringe Benefits",
    # Miscellaneous
    "Monuments and Memorials":              "Monuments and Memorials (Structures)",
    "Trade Shows and Fairs":                "Conventions, Fairs and Trade Shows",
    "Reading and Writing Skills":           "Reading and Writing Skills (Education)",
}

_GENERIC_SUBJECTS = {
    'United States Politics and Government', 'Content Type: Personal Profile',
    'Content Type: Service', 'your-feed-science', 'your-feed-healthcare',
    'your-feed-internet', 'your-feed-animals', 'your-feed-weather',
    'States (US)', 'Research',
}
_GENERIC_PREFIXES = ('internal-', 'audio-', 'vis-', 'your-feed', 'live-', 'durable-uri')
_INSTITUTIONAL_BYLINES = {
    'The New York Times', 'The Associated Press', 'The Editorial Board',
    'The Learning Network', 'New York Times Games', 'International Herald Tribune',
    'Reuters', 'The New York Times Books Staff', 'The Upshot Staff',
    'The Staff', 'The Times Insider Staff', 'The New York Times Sports Staff',
    'The New York Times Staff',
    'Bloomberg News', 'Associated Press', 'Bridge News', 'Field Level Media',
    'Der Spiegel', 'der Spiegel', 'The International Herald Tribune',
    'Br International Herald',
    'New York Times', 'New York Times Audio', 'New York Times Opinion',
    'The New York Times Opinion', 'The New York Times Magazine',
    'The Styles Desk', 'Retro Report', 'New York Times Cooking',
    'Insider Staff', 'the staff of The Morning',
    'Compiled by The New York Times',
    'IFC Films',
    'Written Mr', 'Was Written Mr',
    'Wire Reports', 'From Wire Reports',
    'T Magazine',
    'Courtesy of NBC', 'Courtesy of CBS', 'Courtesy of ABC',
    'From THE NEW YORK TIMES ALMANAC 2004',
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
        # Sort by article count desc (user-visible order = most-written-about first)
        scored.sort(key=lambda x: -freq[x[0]])
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


def is_blog_url(url):
    """Return True if the URL is a blog post."""
    if not url:
        return False
    try:
        domain = url.split('/')[2]
    except IndexError:
        return False
    if domain.endswith('.blogs.nytimes.com') or domain == 'dealbook.nytimes.com':
        return True
    # First Draft: NYT's political blog 2014-2016 (filed under U.S. section,
    # not detected by subdomain check)
    if '/politics/first-draft/' in url:
        return True
    return False


def is_live_url(url):
    """Return True if the URL is a /live/ news-blog entry (2015+).

    Pattern: nytimes.com/live/YYYY/MM/DD/section/slug[#entry]
    Distinct from standard articles — short individual updates (~400-550w avg)
    contributed by reporters to a breaking-news live blog stream.
    """
    if not url:
        return False
    # Must have /live/ as the first path segment after the domain
    try:
        path = url.split('/', 3)[-1]  # everything after https://www.nytimes.com/
        return path.startswith('live/')
    except (IndexError, AttributeError):
        return False


# Kicker strings (case-insensitive, stripped) used to label NYT podcast episodes
# in sections other than "Podcasts" (most often Opinion). Sourced from the kicker
# distribution of the existing Podcasts-section corpus.
_PODCAST_KICKERS = {
    'the daily', 'the ezra klein show', 'still processing', 'the run-up',
    'dear sugars', 'cannonball with wesley morris', 'the new washington',
    "tell me something i don't know", "tell me something i don’t know",
    'the modern love podcast', 'modern love podcast', 'modern love',
    'the book review podcast', 'book review podcast', 'the argument',
    'matter of opinion', 'first person', 'sway', 'hard fork',
    'popcast', 'the popcast', "the 'hard fork' podcast",
    'the opinions', 'interesting times',
}

# Slug fragments that reliably mark Opinion-section podcast posts (e.g. early
# Ezra Klein Show episodes from 2020-21 that have no kicker or section flag).
_PODCAST_SLUG_PATTERNS = (
    'ezra-klein-podcast-', 'argument-podcast-', 'matter-of-opinion-',
)


def is_podcast_article(section, url, kicker):
    """Return True if the article is a podcast episode (not an article *about* podcasts).

    Detection signals (any one is sufficient):
      1. section_name == 'Podcasts'
      2. URL path contains '/podcasts/' or starts with '/audio/'
      3. Kicker matches a known NYT podcast show (Daily, Ezra Klein Show, etc.)
      4. URL slug contains a podcast-specific token (handles 2020-21 Opinion
         episodes that lack both a kicker and the Podcasts section tag)
    """
    if section == 'Podcasts':
        return True
    u = (url or '').lower()
    if '/podcasts/' in u:
        return True
    # /audio/ path prefix (works for both full URLs and stored path-only form)
    if '/audio/' in u and ('nytimes.com/audio/' in u or u.startswith('/audio/')):
        return True
    if kicker and kicker.strip().lower() in _PODCAST_KICKERS:
        return True
    if any(p in u for p in _PODCAST_SLUG_PATTERNS):
        return True
    return False


def deduplicate_articles(articles):
    """
    Remove duplicate articles caused by the 2006 NYT URL scheme transition.

    From ~May 2006, the NYT introduced slug-based URLs alongside the old date-coded format,
    causing the Archive API to index many articles twice under two different URLs.
    Duplicates are detected by matching on (headline, pub_date, word_count ±10%).
    Within each duplicate group, the article with the shorter URL (old date-coded format)
    is retained; the longer slug URL is dropped.
    """
    from collections import defaultdict as _dd

    # Group article indices by (normalized headline, date)
    groups = _dd(list)
    for i, art in enumerate(articles):
        hl = art['headline'].strip().lower()
        if not hl:
            continue
        key = (hl, art['pub_date'][:10])
        groups[key].append(i)

    to_remove = set()
    n_dupes = 0
    for key, indices in groups.items():
        if len(indices) < 2:
            continue
        wcs = [articles[i]['word_count'] for i in indices]
        max_wc = max(wcs)
        if max_wc == 0:
            continue
        min_wc = min(wcs)
        # All word counts within 10% of the max → treat as duplicates
        if min_wc >= max_wc * 0.90:
            # Keep the one with the shortest URL (old date-coded format is shorter)
            sorted_idx = sorted(indices, key=lambda i: len(articles[i]['web_url']))
            for i in sorted_idx[1:]:
                to_remove.add(i)
                n_dupes += 1

    result = [art for i, art in enumerate(articles) if i not in to_remove]
    print(f"  Removed {n_dupes:,} duplicate articles ({n_dupes / len(articles) * 100:.1f}% of total)")
    return result, n_dupes


# Normalize sub-national tags (cities, provinces, regions) to parent country.
# Rules:
#   - City (Country) → Country
#   - Province/State/Region (Country) → Country
#   - ALL-CAPS country names → canonical form
#   - UK sub-nations (England, Scotland, Wales, N. Ireland) → Great Britain
#   - Contested territories kept as-is: Gaza Strip, West Bank, Taiwan, Hong Kong,
#     Tibet, Kashmir, Kosovo, Nagorno-Karabakh, South Ossetia, Abkhazia, Crimea, etc.
#   - Transnational geographies kept as-is: Red Sea, Himalayas, Amazon, etc.
LOCATION_NORMALIZE = {
    # ── Great Britain / UK sub-nations ────────────────────────────────
    "London (England)":           "Great Britain",
    "LONDON (ENG)":               "Great Britain",
    "England":                    "Great Britain",
    "Scotland":                   "Great Britain",
    "Wales":                      "Great Britain",
    "Northern Ireland":           "Great Britain",
    "Manchester (England)":       "Great Britain",
    "Birmingham (England)":       "Great Britain",
    "Liverpool (England)":        "Great Britain",
    "Glasgow (Scotland)":         "Great Britain",
    "Lockerbie (Scotland)":       "Great Britain",
    "Belfast (Northern Ireland)": "Great Britain",
    "Edinburgh (Scotland)":       "Great Britain",
    "Bristol (England)":          "Great Britain",
    "Oxford (England)":           "Great Britain",
    "Cambridge (England)":        "Great Britain",
    # ── France ────────────────────────────────────────────────────────
    "Paris (France)":             "France",
    "Nice (France)":              "France",
    "Marseille (France)":         "France",
    "Calais (France)":            "France",
    "Toulouse (France)":          "France",
    "Normandy (France)":          "France",
    "Lyon (France)":              "France",
    "Bordeaux (France)":          "France",
    # ── Germany ───────────────────────────────────────────────────────
    "Berlin (Germany)":           "Germany",
    "Munich (Germany)":           "Germany",
    "Hamburg (Germany)":          "Germany",
    "Frankfurt (Germany)":        "Germany",
    "Bavaria (Germany)":          "Germany",
    "Cologne (Germany)":          "Germany",
    "East Germany":               "Germany",
    # ── Russia ────────────────────────────────────────────────────────
    "Moscow (Russia)":            "Russia",
    "St Petersburg (Russia)":     "Russia",
    "Chechnya (Russia)":          "Russia",
    "Kursk (Russia)":             "Russia",
    "Sochi (Russia)":             "Russia",
    "Dagestan (Russia)":          "Russia",
    "Siberia":                    "Russia",
    "GROZNY (CHECHNYA)":          "Russia",
    "Ingushetia (Russian Republic)": "Russia",
    "Caucasus (Russia)":          "Russia",
    # ── Ukraine ───────────────────────────────────────────────────────
    "Kyiv (Ukraine)":             "Ukraine",
    "Crimea (Ukraine)":           "Ukraine",
    "Donetsk (Ukraine)":          "Ukraine",
    "Mariupol (Ukraine)":         "Ukraine",
    "Zaporizhzhia (Ukraine)":     "Ukraine",
    "Luhansk (Ukraine)":          "Ukraine",
    "Bakhmut (Ukraine)":          "Ukraine",
    "Odessa (Ukraine)":           "Ukraine",
    "Lviv (Ukraine)":             "Ukraine",
    "Slovyansk (Ukraine)":        "Ukraine",
    "Avdiivka (Ukraine)":         "Ukraine",
    "Pokrovsk (Ukraine)":         "Ukraine",
    "Bucha (Ukraine)":            "Ukraine",
    "Dnipro River (Ukraine)":     "Ukraine",
    "Chernobyl (Ukraine)":        "Ukraine",
    "Kharkiv (Ukraine)":          "Ukraine",
    "Kherson (Ukraine)":          "Ukraine",
    # ── China ─────────────────────────────────────────────────────────
    "Beijing (China)":            "China",
    "Shanghai (China)":           "China",
    "Xinjiang (China)":           "China",
    "Sichuan Province (China)":   "China",
    "Wuhan (China)":              "China",
    "Guangzhou (China)":          "China",
    "Shenzhen (China)":           "China",
    "Chongqing (China)":          "China",
    "Chengdu (China)":            "China",
    "Urumqi (China)":             "China",
    "Tianjin (China)":            "China",
    "Hubei Province (China)":     "China",
    "Henan Province (China)":     "China",
    "Hainan Island (China)":      "China",
    "Zhejiang Province (China)":  "China",
    "Fujian Province (China)":    "China",
    "Yunnan Province (China)":    "China",
    "Nanjing (China)":            "China",
    "Guangdong Province (China)": "China",
    "Kashgar (China)":            "China",
    "Kunming (China)":            "China",
    # ── India ─────────────────────────────────────────────────────────
    "New Delhi (India)":          "India",
    "Mumbai (India)":             "India",
    "Bangalore (India)":          "India",
    "Kashmir and Jammu (India)":  "India",
    "Kolkata (India)":            "India",
    "Chennai (India)":            "India",
    "Hyderabad (India)":          "India",
    "West Bengal (India)":        "India",
    "Tamil Nadu (India)":         "India",
    "Bihar (India)":              "India",
    "Rajasthan (India)":          "India",
    "Kerala (India)":             "India",
    "Karnataka (India)":          "India",
    "Assam State (India)":        "India",
    "Andhra Pradesh (India)":     "India",
    "Maharashtra (India)":        "India",
    "Goa (India)":                "India",
    "Haryana (India)":            "India",
    "Varanasi (India)":           "India",
    "Jaipur (India)":             "India",
    "AHMEDABAD (INDIA)":          "India",
    "Uttar Pradesh (India)":      "India",
    # ── Pakistan ──────────────────────────────────────────────────────
    "Islamabad (Pakistan)":       "Pakistan",
    "Peshawar (Pakistan)":        "Pakistan",
    "Lahore (Pakistan)":          "Pakistan",
    "Waziristan (Pakistan)":      "Pakistan",
    "Baluchistan (Pakistan)":     "Pakistan",
    "Swat (Pakistan)":            "Pakistan",
    "Punjab (Pakistan)":          "Pakistan",
    "Quetta (Pakistan)":          "Pakistan",
    "Karachi (Pakistan)":         "Pakistan",
    "Federally Administered Tribal Areas (Pakistan)": "Pakistan",
    # ── Iraq ──────────────────────────────────────────────────────────
    "Baghdad (Iraq)":             "Iraq",
    "Basra (Iraq)":               "Iraq",
    "Kirkuk (Iraq)":              "Iraq",
    "Karbala (Iraq)":             "Iraq",
    "Ramadi (Iraq)":              "Iraq",
    "Tikrit (Iraq)":              "Iraq",
    "Mosul (Iraq)":               "Iraq",
    "Erbil (Iraq)":               "Iraq",
    "Sadr City (Iraq)":           "Iraq",
    "Nasiriya (Iraq)":            "Iraq",
    "ANBAR PROVINCE (IRAQ)":      "Iraq",
    "Falluja (Iraq)":             "Iraq",
    "Haditha (Iraq)":             "Iraq",
    # ── Syria ─────────────────────────────────────────────────────────
    "Damascus (Syria)":           "Syria",
    "Homs (Syria)":               "Syria",
    "Idlib (Syria)":              "Syria",
    "Raqqa (Syria)":              "Syria",
    "Aleppo (Syria)":             "Syria",
    "Kobani (Syria)":             "Syria",
    "Hama (Syria)":               "Syria",
    "Palmyra (Syria)":            "Syria",
    "Deir al-Zour (Syria)":       "Syria",
    # ── Afghanistan ───────────────────────────────────────────────────
    "Kabul (Afghanistan)":        "Afghanistan",
    "Kandahar (Afghanistan)":     "Afghanistan",
    "Jalalabad (Afghanistan)":    "Afghanistan",
    "Mazar-i-Sharif (Afghanistan)": "Afghanistan",
    "Herat (Afghanistan)":        "Afghanistan",
    "Tora Bora (Afghanistan)":    "Afghanistan",
    "Marja (Afghanistan)":        "Afghanistan",
    "AFGHANISTAN":                "Afghanistan",
    # ── Israel / West Bank / Gaza ─────────────────────────────────────
    "Jerusalem (Israel)":         "Israel",
    "Tel Aviv (Israel)":          "Israel",
    "Haifa (Israel)":             "Israel",
    "JERUSALEM":                  "Israel",
    "Bethlehem (West Bank)":      "West Bank",
    "Nablus (West Bank)":         "West Bank",
    "JENIN (WEST BANK)":          "West Bank",
    "Jenin (West Bank)":          "West Bank",
    "Hebron (West Bank)":         "West Bank",
    "Tulkarm (West Bank)":        "West Bank",
    "Ramallah (West Bank)":       "West Bank",
    "Gaza City (Gaza Strip)":     "Gaza Strip",
    "Khan Younis (Gaza Strip)":   "Gaza Strip",
    "Rafah (Gaza Strip)":         "Gaza Strip",
    "GAZA":                       "Gaza Strip",
    # ── Lebanon ───────────────────────────────────────────────────────
    "Beirut (Lebanon)":           "Lebanon",
    # ── Egypt ─────────────────────────────────────────────────────────
    "Cairo":                      "Egypt",
    "Cairo (Egypt)":              "Egypt",
    "Sinai Peninsula (Egypt)":    "Egypt",
    "Tahrir Square (Cairo)":      "Egypt",
    # ── Turkey ────────────────────────────────────────────────────────
    "Istanbul (Turkey)":          "Turkey",
    "ANKARA (TURKEY)":            "Turkey",
    # ── Iran ──────────────────────────────────────────────────────────
    "Tehran (Iran)":              "Iran",
    # ── Saudi Arabia ──────────────────────────────────────────────────
    "Mecca (Saudi Arabia)":       "Saudi Arabia",
    "Riyadh (Saudi Arabia)":      "Saudi Arabia",
    "Medina (Saudi Arabia)":      "Saudi Arabia",
    # ── UAE ───────────────────────────────────────────────────────────
    "Dubai (United Arab Emirates)": "United Arab Emirates",
    "Abu Dhabi (United Arab Emirates)": "United Arab Emirates",
    # ── Yemen ─────────────────────────────────────────────────────────
    "Sana (Yemen)":               "Yemen",
    "ADEN (YEMEN)":               "Yemen",
    # ── Sudan ─────────────────────────────────────────────────────────
    "Khartoum (Sudan)":           "Sudan",
    "Darfur (Sudan)":             "Sudan",
    "DARFUR PROVINCE (SUDAN)":    "Sudan",
    # ── Libya ─────────────────────────────────────────────────────────
    "Tripoli (Libya)":            "Libya",
    "Benghazi (Libya)":           "Libya",
    "Misurata (Libya)":           "Libya",
    # ── Ethiopia ──────────────────────────────────────────────────────
    "Tigray (Ethiopia)":          "Ethiopia",
    "ADDIS ABABA (ETHIOPIA)":     "Ethiopia",
    # ── Nigeria ───────────────────────────────────────────────────────
    "Lagos (Nigeria)":            "Nigeria",
    # ── South Africa ──────────────────────────────────────────────────
    "Johannesburg (South Africa)":"South Africa",
    "Cape Town (South Africa)":   "South Africa",
    "Pretoria (South Africa)":    "South Africa",
    # ── Congo ─────────────────────────────────────────────────────────
    "CONGO":                      "Democratic Republic of Congo",
    # ── Kenya ─────────────────────────────────────────────────────────
    "Mombasa (Kenya)":            "Kenya",
    "Nairobi (Kenya)":            "Kenya",
    # ── Japan ─────────────────────────────────────────────────────────
    "Tokyo (Japan)":              "Japan",
    "Hiroshima (Japan)":          "Japan",
    "Osaka (Japan)":              "Japan",
    "Okinawa and Other Ryukyu Islands (Japan)": "Japan",
    # ── South Korea ───────────────────────────────────────────────────
    "Seoul (South Korea)":        "South Korea",
    "Panmunjom (South Korea)":    "South Korea",
    # ── North Korea ───────────────────────────────────────────────────
    "Pyongyang (North Korea)":    "North Korea",
    # ── Vietnam ───────────────────────────────────────────────────────
    "Hanoi (Vietnam)":            "Vietnam",
    "Ho Chi Minh City (Vietnam)": "Vietnam",
    # ── Myanmar ───────────────────────────────────────────────────────
    "Yangon (Myanmar)":           "Myanmar",
    "Rakhine State (Myanmar)":    "Myanmar",
    # ── Malaysia ──────────────────────────────────────────────────────
    "Kuala Lumpur (Malaysia)":    "Malaysia",
    # ── Philippines ───────────────────────────────────────────────────
    "Mindanao (Philippines)":     "Philippines",
    "Tacloban (Philippines)":     "Philippines",
    # ── Indonesia ─────────────────────────────────────────────────────
    "ACEH PROVINCE (INDONESIA)":  "Indonesia",
    "Java (Indonesia)":           "Indonesia",
    # ── Bangladesh ────────────────────────────────────────────────────
    "Dhaka (Bangladesh)":         "Bangladesh",
    # ── Nepal ─────────────────────────────────────────────────────────
    "Katmandu (Nepal)":           "Nepal",
    # ── Sri Lanka ─────────────────────────────────────────────────────
    "Colombo (Sri Lanka)":        "Sri Lanka",
    # ── Australia ─────────────────────────────────────────────────────
    "New South Wales (Australia)":"Australia",
    "Queensland (Australia)":     "Australia",
    "Victoria (Australia)":       "Australia",
    "Canberra (Australia)":       "Australia",
    "Bondi Beach (Sydney, Australia)": "Australia",
    "Great Barrier Reef (Australia)": "Australia",
    # ── New Zealand ───────────────────────────────────────────────────
    "Christchurch (New Zealand)": "New Zealand",
    "Auckland (New Zealand)":     "New Zealand",
    # ── Canada ────────────────────────────────────────────────────────
    "Montreal (Quebec)":          "Canada",
    "Ottawa (Ontario)":           "Canada",
    "Vancouver (British Columbia)": "Canada",
    "Toronto (Ontario)":          "Canada",
    "ALBERTA (CANADA)":           "Canada",
    "Saskatchewan (Canada)":      "Canada",
    # ── Mexico ────────────────────────────────────────────────────────
    "Mexico City (Mexico)":       "Mexico",
    "Tijuana (Mexico)":           "Mexico",
    "Ciudad Juarez (Mexico)":     "Mexico",
    "Oaxaca (Mexico)":            "Mexico",
    "Chiapas (Mexico)":           "Mexico",
    "Guerrero (Mexico)":          "Mexico",
    "Baja California (Mexico)":   "Mexico",
    # ── Argentina ─────────────────────────────────────────────────────
    "ARGENTINA":                  "Argentina",
    "Buenos Aires (Argentina)":   "Argentina",
    # ── Brazil ────────────────────────────────────────────────────────
    "Sao Paulo (Brazil)":         "Brazil",
    # ── Colombia ──────────────────────────────────────────────────────
    "Bogota (Colombia)":          "Colombia",
    # ── Peru ──────────────────────────────────────────────────────────
    "Lima (Peru)":                "Peru",
    # ── Cuba ──────────────────────────────────────────────────────────
    "Havana (Cuba)":              "Cuba",
    # ── Haiti ─────────────────────────────────────────────────────────
    "Port-au-Prince (Haiti)":     "Haiti",
    # ── Dominican Republic ────────────────────────────────────────────
    # (standalone, keep)
    # ── Serbia ────────────────────────────────────────────────────────
    "Belgrade (Serbia)":          "Serbia",
    "KOSOVO (SERBIA)":            "Kosovo",
    # ── Hungary ───────────────────────────────────────────────────────
    "Budapest (Hungary)":         "Hungary",
    # ── Czech Republic ────────────────────────────────────────────────
    "Prague (Czech Republic)":    "Czech Republic",
    # ── Poland ────────────────────────────────────────────────────────
    "Warsaw (Poland)":            "Poland",
    # ── Romania ───────────────────────────────────────────────────────
    "Bucharest (Romania)":        "Romania",
    # ── Austria ───────────────────────────────────────────────────────
    "Vienna (Austria)":           "Austria",
    # ── Belgium ───────────────────────────────────────────────────────
    "Brussels (Belgium)":         "Belgium",
    "AMSTERDAM (NETHERLANDS)":    "Netherlands",
    # ── Spain ─────────────────────────────────────────────────────────
    "Barcelona (Spain)":          "Spain",
    "Madrid (Spain)":             "Spain",
    "Valencia (Spain)":           "Spain",
    # ── Italy ─────────────────────────────────────────────────────────
    "Rome (Italy)":               "Italy",
    "Venice (Italy)":             "Italy",
    "Milan (Italy)":              "Italy",
    "Sicily (Italy)":             "Italy",
    "Naples (Italy)":             "Italy",
    "Florence (Italy)":           "Italy",
    "Genoa (Italy)":              "Italy",
    "Tuscany (Italy)":            "Italy",
    # ── Sweden ────────────────────────────────────────────────────────
    "Stockholm (Sweden)":         "Sweden",
    # ── Norway ────────────────────────────────────────────────────────
    "Oslo (Norway)":              "Norway",
    # ── Denmark ───────────────────────────────────────────────────────
    "Copenhagen (Denmark)":       "Denmark",
    # ── Ireland ───────────────────────────────────────────────────────
    "Dublin (Ireland)":           "Ireland",
    # ── Switzerland ───────────────────────────────────────────────────
    "Geneva (Switzerland)":       "Switzerland",
    "Davos (Switzerland)":        "Switzerland",
    "Zurich (Switzerland)":       "Switzerland",
    # ── Georgia ───────────────────────────────────────────────────────
    "Georgia (Georgian Republic)":"Georgia",
    "South Ossetia (Georgian Republic)": "Georgia",
    "ABKHAZIA (GEORGIAN REPUBLIC)": "Georgia",
    # ── Jordan ────────────────────────────────────────────────────────
    "AMMAN (JORDAN)":             "Jordan",
    # ── Qatar ─────────────────────────────────────────────────────────
    "Doha (Qatar)":               "Qatar",
    # ── Belarus ───────────────────────────────────────────────────────
    "Minsk (Belarus)":            "Belarus",
    # ── Ukraine (Russia)  ─────────────────────────────────────────────
    # ── Liberia ───────────────────────────────────────────────────────
    "Monrovia (Liberia)":         "Liberia",
    # ── Vietnam ───────────────────────────────────────────────────────
    # ── Senegal ───────────────────────────────────────────────────────
    "Dakar (Senegal)":            "Senegal",
    # ── Zimbabwe ──────────────────────────────────────────────────────
    "Harare (Zimbabwe)":          "Zimbabwe",
    # ── Bosnia ────────────────────────────────────────────────────────
    "SREBRENICA (BOSNIA)":        "Bosnia and Herzegovina",
    # ── Macedonia ─────────────────────────────────────────────────────
    "MACEDONIA (FORMER YUGOSLAV REPUBLIC)": "Macedonia",
    # ── ALL-CAPS country name fixes ───────────────────────────────────
    "AFRICA":                     "Africa",
    "ALGERIA":                    "Algeria",
    "ARMENIA":                    "Armenia",
    "ANGOLA":                     "Angola",
    "ALBANIA":                    "Albania",
    "WASHINGTON":                 "United States",
    "USSR (Former Soviet Union)": "Russia",
    "Yugoslavia":                 "Serbia",
    # ── US cities (World section articles mentioning US places) ───────
    "Los Angeles (Calif)":        "United States",
    "Chicago (Ill)":              "United States",
    "Miami (Fla)":                "United States",
    "Boston (Mass)":              "United States",
    "California":                 "United States",
    "Texas":                      "United States",
    "Florida":                    "United States",
    "New Jersey":                 "United States",
    "New York State":             "United States",
    "Manhattan (NYC)":            "United States",
    "New York City":              "United States",
    "Washington (DC)":            "United States",
    # ── Extra fixes not caught by programmatic rule ────────────────────
    "ALEPPO (SYRIA)":             "Syria",
    "Congo (Formerly Zaire)":     "Democratic Republic of Congo",
    "KASHMIR AND JAMMU":          "Kashmir",
    "SERBIA AND MONTENEGRO":      "Serbia",
    "Tiananmen Square (Beijing)": "China",
    "Guantanamo Bay Naval Base (Cuba)": "Cuba",
    "Kosovo (Serbia)":            "Kosovo",
    "DUBAI":                      "United Arab Emirates",
    "Soviet Union":               "Russia",
    # ── Standalone US states → United States ──────────────────────────
    "Alabama": "United States", "Alaska": "United States",
    "Arizona": "United States", "Arkansas": "United States",
    "Colorado": "United States", "Connecticut": "United States",
    "Delaware": "United States", "Hawaii": "United States",
    "Idaho": "United States", "Illinois": "United States",
    "Indiana": "United States", "Iowa": "United States",
    "Kansas": "United States", "Kentucky": "United States",
    "Louisiana": "United States", "Maryland": "United States",
    "Massachusetts": "United States", "Michigan": "United States",
    "Minnesota": "United States", "Mississippi": "United States",
    "Missouri": "United States", "Montana": "United States",
    "Nebraska": "United States", "New Hampshire": "United States",
    "New Mexico": "United States", "North Carolina": "United States",
    "Ohio": "United States", "Oklahoma": "United States",
    "Oregon": "United States", "Pennsylvania": "United States",
    "South Carolina": "United States", "Tennessee": "United States",
    "Utah": "United States", "Vermont": "United States",
    "Virginia": "United States", "West Virginia": "United States",
    "Wisconsin": "United States", "Wyoming": "United States",
    "Washington (State)": "United States",
    # ── US cities with state abbreviations not caught by PARENT_MAP ───
    "Atlanta (Ga)": "United States", "Ellabell (Ga)": "United States",
    "Austin (Tex)": "United States", "Dallas (Tex)": "United States",
    "Houston (Tex)": "United States", "Fort Hood (Tex)": "United States",
    "El Paso (Tex)": "United States",
    "Cleveland (Ohio)": "United States",
    "Detroit (Mich)": "United States", "Dearborn (Mich)": "United States",
    "Minneapolis (Minn)": "United States",
    "Ferguson (Mo)": "United States",
    "Las Vegas (Nev)": "United States",
    "New Orleans (La)": "United States",
    "Philadelphia (Pa)": "United States", "Pittsburgh (Pa)": "United States",
    "Charlottesville (Va)": "United States",
    "Seattle (Wash)": "United States",
    "Joint Base Lewis-McChord (Wash)": "United States",
    "Salt Lake City (Utah)": "United States",
    "Newark (NJ)": "United States", "Long Island (NY)": "United States",
    "Baltimore (Md)": "United States", "Annapolis (MD)": "United States",
    "ANNAPOLIS (MD)": "United States", "CAMP DAVID (MD)": "United States",
    "Shanksville (PA)": "United States",
    "Marine Corps Base Camp Lejeune (SC)": "United States",
    "Pearl Harbor (Hawaii)": "United States",
    "Honolulu (Hawaii)": "United States",
    "Northeastern States (US)": "United States",
    "Southern States (US)": "United States",
    "Ambassador Bridge": "United States",
    "Central Park (Manhattan, NY)": "United States",
    "ALCATRAZ (SAN FRANCISCO)": "United States",
    # ── Standalone world cities → country ─────────────────────────────
    "Beijing": "China", "BEIJING": "China",
    "Moscow": "Russia", "MOSCOW": "Russia",
    "Berlin": "Germany", "BERLIN": "Germany",
    "Paris": "France",
    "Rome": "Italy", "ROME": "Italy",
    "London": "Great Britain", "LONDON": "Great Britain",
    "TEL AVIV": "Israel",
    "BAGHDAD": "Iraq",
    "Geneva": "Switzerland",
    "Belgrade": "Serbia", "BELGRADE": "Serbia",
    "OKINAWA": "Japan", "OKINAWA AND OTHER RYUKYU ISLANDS": "Japan",
    "SICILY": "Italy",
    "HONG KONG (CHINA)": "Hong Kong",
    "AMERICAN SAMOA": "United States",
    # ── Country name variants / outdated names ─────────────────────────
    "Burma": "Myanmar", "BURMA": "Myanmar",
    "Britain": "Great Britain",
    "The United States": "United States",
    "America": "United States",
    "Rhodesia": "Zimbabwe",
    "Republic of Biafra": "Nigeria",
    "Yugoslavia": "Serbia",
    "EUROPEAN UNION": "Europe",
    "Macao": "Macau",
    "Swaziland": "Eswatini",
    "Czechoslovakia": "Czech Republic",
    "CZECHOSLOVAKIA (PRE-1993)": "Czech Republic",
    # ── Too-granular places → parent country ──────────────────────────
    "Red Square (Moscow)": "Russia",
    "Western Wall (Jerusalem)": "Israel",
    "Champs-Elysees (Paris)": "France",
    "11th Arrondissement (Paris, France)": "France",
    "Pont des Arts Bridge (Paris, France)": "France",
    "Morandi Bridge (Genoa, Italy)": "Italy",
    "Taksim Square (Istanbul, Turkey)": "Turkey",
    "Great Wall of China": "China",
    "Grindavik (Iceland)": "Iceland",
    "Eyjafjallajokull Volcano (Iceland)": "Iceland",
    "Kerch Strait Bridge": "Russia",
    "Negev Desert": "Israel",
    "Sinai Peninsula": "Egypt",
    "Suez Canal": "Egypt",
    "Tiran Island": "Egypt",
    "Sanafir Island": "Egypt",
    "Inner Mongolia": "China",
    "Panama Canal and Canal Zone": "Panama",
    "Panama City (Panama)": "Panama",
    # ── Territories/dependencies → parent country ──────────────────────
    "French Guiana": "France",
    "Guadeloupe": "France",
    "New Caledonia": "France",
    "Reunion Island": "France",
    "Tahiti": "France",
    "Corsica": "France",
    "Mayotte (Comoro Islands)": "France",  # Mayotte is French, not Comorian
    "French Alps": "France",
    "Guam": "United States",
    "Virgin Islands (US)": "United States",
    "Virgin Islands (Great Britain)": "Great Britain",
    "Anguilla": "Great Britain",
    "Bermuda": "Great Britain",
    "Cayman Islands": "Great Britain",
    "Channel Islands": "Great Britain",
    "Falkland Islands": "Great Britain",
    "Gibraltar": "Great Britain",
    "Isle of Man": "Great Britain",
    "Northern Ireland (United Kingdom)": "Great Britain",
    "LONDONDERRY (NORTHERN IRELAND)": "Great Britain",
    "PORTADOWN (NORTHERN IRELAND)": "Great Britain",
    "Aruba": "Netherlands",
    "Curacao": "Netherlands",
    "Greenland": "Denmark",
    "Faroe Islands": "Denmark",
    "Canary Islands": "Spain",
    "Azores Islands": "Portugal",
    "Andaman Islands": "India",
    "Galapagos Islands": "Ecuador",
    "Easter Island": "Chile",
    "Grand Bahama Island": "Bahamas",
    "Bahama Islands": "Bahamas",
    "ABACO ISLANDS (BAHAMAS)": "Bahamas",
    # ── Cities needing explicit entries ────────────────────────────────
    "Halifax (Nova Scotia)": "Canada",
    "MONTREAL (CANADA)": "Canada",
    "ONTARIO PROVINCE (CANADA)": "Canada",
    "Humboldt (Saskatchewan)": "Canada",
    "Hobart (Tasmania)": "Australia",
    "Kowloon (Hong Kong)": "Hong Kong",
    "Yuen Long (Hong Kong)": "Hong Kong",
    "HONG KONG (CHINA)": "Hong Kong",
    "Jamaica (West Indies)": "Jamaica",
    "Kinshasa (Democratic Republic of Congo)": "Democratic Republic of Congo",
    "Stepanakert (Nagorno-Karabakh Republic)": "Nagorno-Karabakh",
    "SARAJEVO (BOSNIA)": "Bosnia and Herzegovina",
    "BOSNIA": "Bosnia and Herzegovina",
    "Chechnya": "Russia",
    "CONGO REPUBLIC": "Republic of Congo",
    "Congo, Republic of (Congo-Brazzaville)": "Republic of Congo",
    "BRAZZAVILLE (CONGO REPUBLIC)": "Republic of Congo",
    "GOUALOGO TRIANGLE (CONGO REPUBLIC)": "Republic of Congo",
    "OKAPI FAUNAL RESERVE (REPUBLIC OF CONGO)": "Republic of Congo",
    "Democratic Federation of Rojava-North Syria": "Syria",
    "Republic of North Macedonia": "North Macedonia",
    "TETOVO (MACEDONIA)": "North Macedonia",
    "Kurile Islands": "Russia",
    "SAKHALIN ISLAND": "Russia",
    # Remaining city→country entries whose parents need explicit mapping
    "Dili (East Timor)": "East Timor",
    "Bangui (Central African Republic)": "Central African Republic",
    "Nuuk (Greenland)": "Denmark",
    "St Martin (Caribbean)": "Caribbean Area",
    "Brixton (London, England)": "Great Britain",
    "Sevnica (Slovenia)": "Slovenia",
    "Yellowknife (Northwest Territories)": "Canada",
}

# Programmatic rule: any "Name (Parent)" where Parent maps to a known country.
# Applied AFTER the explicit dict, as a fallback for the thousands of remaining
# sub-national tags (cities, provinces, regions) following this pattern.
PARENT_MAP = {
    "India": "India", "China": "China", "Ukraine": "Ukraine", "Russia": "Russia",
    "Iraq": "Iraq", "Syria": "Syria", "Afghanistan": "Afghanistan",
    "Pakistan": "Pakistan", "Israel": "Israel", "West Bank": "West Bank",
    "Gaza Strip": "Gaza Strip", "Turkey": "Turkey", "Iran": "Iran",
    "Saudi Arabia": "Saudi Arabia", "Egypt": "Egypt", "Lebanon": "Lebanon",
    "Jordan": "Jordan", "Nigeria": "Nigeria", "South Africa": "South Africa",
    "Kenya": "Kenya", "Somalia": "Somalia", "Libya": "Libya", "Sudan": "Sudan",
    "Ethiopia": "Ethiopia", "Venezuela": "Venezuela", "Colombia": "Colombia",
    "Mexico": "Mexico", "Brazil": "Brazil", "Argentina": "Argentina",
    "Peru": "Peru", "Cuba": "Cuba", "Haiti": "Haiti", "Indonesia": "Indonesia",
    "Philippines": "Philippines", "Myanmar": "Myanmar", "Malaysia": "Malaysia",
    "Vietnam": "Vietnam", "Bangladesh": "Bangladesh", "Nepal": "Nepal",
    "Sri Lanka": "Sri Lanka", "Australia": "Australia",
    "New Zealand": "New Zealand", "Canada": "Canada", "Thailand": "Thailand",
    "Netherlands": "Netherlands", "Belgium": "Belgium", "Spain": "Spain",
    "Italy": "Italy", "France": "France", "Germany": "Germany",
    "England": "Great Britain", "Scotland": "Great Britain",
    "Wales": "Great Britain", "Northern Ireland": "Great Britain",
    "Greece": "Greece", "Serbia": "Serbia", "Hungary": "Hungary",
    "Czech Republic": "Czech Republic", "Poland": "Poland",
    "Romania": "Romania", "Austria": "Austria", "Switzerland": "Switzerland",
    "Sweden": "Sweden", "Norway": "Norway", "Denmark": "Denmark",
    "Ireland": "Ireland", "Georgian Republic": "Georgia", "Georgia": "Georgia",
    "Qatar": "Qatar", "Belarus": "Belarus", "Taiwan": "Taiwan",
    "North Korea": "North Korea", "South Korea": "South Korea", "Japan": "Japan",
    "Yemen": "Yemen", "Morocco": "Morocco", "Algeria": "Algeria",
    "Tunisia": "Tunisia", "Zimbabwe": "Zimbabwe", "Uganda": "Uganda",
    "Rwanda": "Rwanda", "Liberia": "Liberia", "Papua New Guinea": "Papua New Guinea",
    "Angola": "Angola", "Mozambique": "Mozambique", "Tanzania": "Tanzania",
    "Ghana": "Ghana", "Senegal": "Senegal", "Cameroon": "Cameroon",
    "Ivory Coast": "Ivory Coast", "Mali": "Mali", "Niger": "Niger",
    "Burkina Faso": "Burkina Faso", "Malawi": "Malawi", "Zambia": "Zambia",
    "Madagascar": "Madagascar", "Eritrea": "Eritrea", "Djibouti": "Djibouti",
    "Bosnia and Herzegovina": "Bosnia and Herzegovina",
    "Kosovo": "Kosovo", "Macedonia": "Macedonia", "Montenegro": "Montenegro",
    "Kazakhstan": "Kazakhstan", "Uzbekistan": "Uzbekistan",
    "Kyrgyzstan": "Kyrgyzstan", "Tajikistan": "Tajikistan",
    "Turkmenistan": "Turkmenistan", "Azerbaijan": "Azerbaijan",
    "Armenia": "Armenia", "Moldova": "Moldova",
    "United Arab Emirates": "United Arab Emirates",
    "Oman": "Oman", "Kuwait": "Kuwait", "Bahrain": "Bahrain",
    "Singapore": "Singapore", "Cambodia": "Cambodia", "Laos": "Laos",
    "Congo, Democratic Republic of": "Democratic Republic of Congo",
    "Democratic Republic of the Congo": "Democratic Republic of Congo",
    "Beijing": "China",   # e.g. "Tiananmen Square (Beijing)"
    "NYC": "United States",
    # All US state abbreviations (catch any remaining "City (Abbr)" patterns)
    "Ala": "United States", "Alaska": "United States",
    "Ariz": "United States", "Ark": "United States",
    "Calif": "United States", "Colo": "United States",
    "Conn": "United States", "Del": "United States",
    "Fla": "United States", "Ga": "United States",
    "Hawaii": "United States", "Idaho": "United States",
    "Ill": "United States", "Ind": "United States",
    "Iowa": "United States", "Kan": "United States",
    "Ky": "United States", "La": "United States",
    "Me": "United States", "Md": "United States",
    "Mass": "United States", "Mich": "United States",
    "Minn": "United States", "Miss": "United States",
    "Mo": "United States", "Mont": "United States",
    "Neb": "United States", "Nev": "United States",
    "NH": "United States", "NJ": "United States",
    "NM": "United States", "NY": "United States",
    "NC": "United States", "ND": "United States",
    "Ohio": "United States", "Okla": "United States",
    "Ore": "United States", "Pa": "United States",
    "RI": "United States", "SC": "United States",
    "SD": "United States", "Tenn": "United States",
    "Tex": "United States", "Utah": "United States",
    "Vt": "United States", "Va": "United States",
    "Wash": "United States", "WVa": "United States",
    "Wis": "United States", "Wyo": "United States",
    "Texas": "United States",
    "Ontario": "Canada", "Quebec": "Canada", "British Columbia": "Canada",
    "Alberta": "Canada", "Manitoba": "Canada", "Newfoundland": "Canada",
    "Nova Scotia": "Canada", "Saskatchewan": "Canada", "Yukon": "Canada",
    "Northwest Territories": "Canada",
    # UK abbreviations used in old all-caps tags
    "Eng": "Great Britain", "England": "Great Britain",
    "Ger": "Germany", "Germany": "Germany",
    "Mex": "Mexico",
    "Gaza": "Gaza Strip",
    "Indian State": "India",
    "West Indies": "Caribbean Area",
    "Congo": "Democratic Republic of Congo",
    "Bahamas": "Bahamas",
    "Tasmania": "Australia",
    "United Kingdom": "Great Britain",
    "Greenland": "Denmark",
    "Caribbean": "Caribbean Area",
    # Countries missing from original PARENT_MAP
    "Portugal": "Portugal", "Paraguay": "Paraguay", "Bolivia": "Bolivia",
    "Croatia": "Croatia", "Slovenia": "Slovenia", "Slovakia": "Slovakia",
    "Latvia": "Latvia", "Estonia": "Estonia", "Lithuania": "Lithuania",
    "Albania": "Albania", "Bulgaria": "Bulgaria", "Finland": "Finland",
    "Iceland": "Iceland", "Uruguay": "Uruguay", "Chile": "Chile",
    "Ecuador": "Ecuador", "Guatemala": "Guatemala", "Honduras": "Honduras",
    "Nicaragua": "Nicaragua", "Dominican Republic": "Dominican Republic",
    "Burundi": "Burundi", "Sierra Leone": "Sierra Leone", "Guyana": "Guyana",
    "Solomon Islands": "Solomon Islands", "Jamaica": "Jamaica",
    "South Sudan": "South Sudan", "Chad": "Chad",
    "East Timor": "East Timor", "Timor-Leste": "East Timor",
    "Central African Republic": "Central African Republic",
    # Kashmir/Tibet — contested territories, map cities to territory name
    "Kashmir": "Kashmir", "Jammu and Kashmir": "Kashmir",
    "Kashmir and Jammu": "Kashmir",
    "Tibet": "Tibet",
    # Misc
    "London, England": "Great Britain",
    "South China Sea": "South China Sea",  # keep Scarborough Shoal → South China Sea
}

# Entries that cannot be meaningfully mapped to a country/territory and
# should be dropped entirely from the world locations list.
DROP_LOCS = {
    "VIETNAM WAR",          # not a place
    "Korean",               # malformed tag (not the country)
    "Silk Road (Ancient Trade Route)",  # historical concept
    "Channel Tunnel",       # infrastructure
    "Mont Blanc",           # mountain
    "Mount Everest",        # mountain
    "K2 (Himalayas)",       # mountain
    "ELBE (RIVER)",         # river, too granular
}

import re as _re
# Greedy prefix + [^)]+ capture so "City (Sub) (Country)" extracts "Country" not "Sub) (Country"
_paren_re = _re.compile(r'^.+\(([^)]+)\)$')

def _normalize_loc(loc):
    if loc in DROP_LOCS:
        return None
    # Direct lookup
    if loc in LOCATION_NORMALIZE:
        return LOCATION_NORMALIZE[loc]
    # Title-cased lookup (handles ALL-CAPS legacy tags like "NORTH KOREA")
    loc_title = loc.title()
    if loc != loc_title and loc_title in LOCATION_NORMALIZE:
        return LOCATION_NORMALIZE[loc_title]
    # Parenthetical pattern: "City (Parent)" or "City (Sub) (Country)"
    m = _paren_re.match(loc)
    if m:
        parent = m.group(1).strip()
        # Try both original case and title case (ALL-CAPS parent tags)
        for pk in (parent, parent.title()):
            if pk in PARENT_MAP:
                return PARENT_MAP[pk]
    # Title-case if still entirely ALL-CAPS (handles remaining unknown all-caps tags)
    if loc == loc.upper() and len(loc) > 2 and any(c.isalpha() for c in loc):
        return loc_title
    # Catch remaining DRC name variants (e.g. "Congo, The Democratic Republic of the")
    loc_lower = loc.lower()
    if 'democratic' in loc_lower and 'congo' in loc_lower:
        return "Democratic Republic of Congo"
    return loc


def build_dashboard_data(articles, authors):
    """Pre-compute dashboard statistics."""
    # Articles per month — split into blog / podcast / standard (mutually exclusive,
    # blog takes precedence over podcast for the rare overlap)
    monthly = Counter()
    monthly_words = defaultdict(int)
    monthly_blog = Counter()
    monthly_blog_words = defaultdict(int)
    monthly_podcast = Counter()
    monthly_podcast_words = defaultdict(int)
    monthly_standard = Counter()
    annual_wc_list = defaultdict(list)  # standard articles only, for exact median
    for art in articles:
        ym = art["year_month"]
        wc = art["word_count"]
        monthly[ym] += 1
        monthly_words[ym] += wc
        is_blog = is_blog_url(art["web_url"])
        is_pod = is_podcast_article(art.get("section"), art.get("web_url", ""), art.get("kicker", ""))
        is_live = is_live_url(art.get("web_url", "")) or art.get("type_of_material") == "Brief"
        if is_blog:
            monthly_blog[ym] += 1
            monthly_blog_words[ym] += wc
        elif is_pod:
            monthly_podcast[ym] += 1
            monthly_podcast_words[ym] += wc
        else:
            monthly_standard[ym] += 1
        # Accumulate annual word counts for STANDARD articles only (no blogs/podcasts/live/briefs)
        if wc > 0 and not is_blog and not is_pod and not is_live:
            annual_wc_list[ym[:4]].append(wc)

    # Compute exact annual median words from full word-count lists
    def exact_median(wc_list):
        if not wc_list: return 0
        s = sorted(wc_list)
        n = len(s)
        mid = n // 2
        return s[mid] if n % 2 else (s[mid - 1] + s[mid]) // 2
    annual_median_words = {y: exact_median(lst) for y, lst in annual_wc_list.items()}

    months_sorted = sorted(monthly.keys())
    articles_per_month = [
        {
            "month": m,
            "count": monthly[m],
            "words": monthly_words[m],
            "blog": monthly_blog[m],
            "blog_words": monthly_blog_words[m],
            "podcast": monthly_podcast[m],
            "podcast_words": monthly_podcast_words[m],
            # `nonblog` retained for backward compatibility — it now means "non-blog AND non-podcast"
            "nonblog": monthly_standard[m],
            "median_words": annual_median_words.get(m[:4], 0),
        }
        for m in months_sorted
    ]

    # Section stats
    section_counts = Counter()
    section_words = defaultdict(int)
    section_wc_hist = defaultdict(lambda: [0] * 21)   # 21 bins × 200 words, last = 4000+
    section_nonzero = defaultdict(int)                 # articles with word_count > 0
    for art in articles:
        s = art["section"] or "(none)"
        section_counts[s] += 1
        section_words[s] += art["word_count"]
        wc = art["word_count"] or 0
        if wc > 0:
            section_nonzero[s] += 1
            bin_idx = min(wc // 200, 20)
            section_wc_hist[s][bin_idx] += 1

    sections = []
    for s, count in section_counts.most_common():
        sections.append({
            "name": s,
            "count": count,
            "total_words": section_words[s],
            "avg_words": round(section_words[s] / section_nonzero[s]) if section_nonzero[s] else 0,
            "wc_hist": section_wc_hist[s],
        })

    # Words per section over time (all sections)
    top_sections = [s["name"] for s in sections if s["name"] not in ("", "(none)")]
    section_time = defaultdict(lambda: defaultdict(lambda: {"count": 0, "words": 0, "wc_list": [], "wc_hist": [0] * 21}))
    for art in articles:
        s = art["section"]
        if s in top_sections:
            y = str(art["year"])
            section_time[s][y]["count"] += 1
            section_time[s][y]["words"] += art["word_count"]
            wc = art["word_count"] or 0
            if wc > 0:
                section_time[s][y]["wc_list"].append(wc)
                section_time[s][y]["wc_hist"][min(wc // 200, 20)] += 1

    # --- Inferred Movies for 2005: in that year the API filed movie reviews under "Arts"  ---
    # Identify "core movie reviewers" = authors with >=20 Movies articles in 2004 OR 2006.
    # Then count their Arts-section articles with "motion pictures" in subjects in 2005
    # as inferred Movies content.
    movie_reviewer_counts = Counter()
    for art in articles:
        if art["section"] == "Movies" and art["year"] in (2004, 2006):
            for auth in art["authors"]:
                movie_reviewer_counts[auth] += 1
    core_movie_reviewers = {name for name, c in movie_reviewer_counts.items() if c >= 20}
    inferred_movies_2005 = sum(
        1 for art in articles
        if art["year"] == 2005
        and art["section"] == "Arts"
        and any(a in core_movie_reviewers for a in art["authors"])
        and any("motion pictures" in s.lower() for s in art.get("subjects", []))
    )

    section_trends = {}
    all_years = sorted(set(str(a["year"]) for a in articles))
    # wc_hist_by_year: per-section dict of {year: [21 bins]} for the popup's year-layered line chart
    section_wc_hist_by_year = {}
    for s in top_sections:
        trend = []
        by_year = {}
        for y in all_years:
            d = section_time[s][y]
            avg = round(d["words"] / d["count"]) if d["count"] else 0
            wc = sorted(d["wc_list"])
            n = len(wc)
            median = round(wc[n // 2] if n % 2 else (wc[n // 2 - 1] + wc[n // 2]) / 2) if wc else 0
            entry = {"year": y, "count": d["count"], "avg_words": avg, "median_words": median}
            # Tag the inferred 2005 Movies gap
            if s == "Movies" and y == "2005" and inferred_movies_2005 > 0:
                entry["inferred_count"] = inferred_movies_2005
            trend.append(entry)
            if sum(d["wc_hist"]) > 0:
                by_year[y] = d["wc_hist"]
        section_trends[s] = trend
        section_wc_hist_by_year[s] = by_year

    # Attach wc_hist_by_year onto each section record
    for sec in sections:
        sec["wc_hist_by_year"] = section_wc_hist_by_year.get(sec["name"], {})

    # Build an "All Sections" aggregate entry (placed first so UI can separate it)
    all_wc_hist = [0] * 21
    all_wc_hist_by_year = defaultdict(lambda: [0] * 21)
    total_articles = 0
    total_words_all = 0
    for art in articles:
        wc = art["word_count"] or 0
        total_articles += 1
        total_words_all += wc
        if wc > 0:
            b = min(wc // 200, 20)
            all_wc_hist[b] += 1
            all_wc_hist_by_year[str(art["year"])][b] += 1
    all_sections_entry = {
        "name": "All Sections",
        "count": total_articles,
        "total_words": total_words_all,
        "avg_words": round(total_words_all / total_articles) if total_articles else 0,
        "wc_hist": all_wc_hist,
        "wc_hist_by_year": dict(all_wc_hist_by_year),
        "is_aggregate": True,
    }
    # Build section_trend for All Sections (articles, avg, median per year).
    # Group by year in one pass to avoid scanning all articles once per year.
    _wcs_by_year = defaultdict(list)
    for a in articles:
        _wcs_by_year[str(a["year"])].append(a["word_count"])
    all_trend = []
    for y in all_years:
        wcs = _wcs_by_year.get(y, [])
        cnt = len(wcs)
        wc_list = sorted(wc for wc in wcs if wc > 0)
        n = len(wc_list)
        words = sum(wcs)
        avg = round(words / cnt) if cnt else 0
        median = round(wc_list[n // 2] if n % 2 else (wc_list[n // 2 - 1] + wc_list[n // 2]) / 2) if wc_list else 0
        all_trend.append({"year": y, "count": cnt, "avg_words": avg, "median_words": median})
    section_trends["All Sections"] = all_trend

    # Top 25 authors (by article count, 25+ articles)
    top_authors = [a for a in authors if a["article_count"] >= 25][:50]

    # Photo grid (top-100) needs 2025 stats for every name in the manifest, but
    # top_authors is capped at 50 — so emit a separate lookup for the grid.
    try:
        with open("data/bio_photos/manifest.json") as fh:
            _manifest = json.load(fh)
        _names = {e["name"] for e in _manifest}
        _grid_stats = {}
        for a in authors:
            if a["name"] not in _names:
                continue
            arts_2025 = sum(c for mo, c in (a.get("monthly_counts") or {}).items() if mo.startswith("2025-"))
            aw = a.get("annual_words") or {}
            words_2025 = aw.get(2025, aw.get("2025", 0))
            _grid_stats[a["name"]] = {"articles": arts_2025, "words": words_2025}
        with open("data/photo_grid_stats.json", "w") as fh:
            json.dump(_grid_stats, fh, separators=(",", ":"))
        _nz = sum(1 for v in _grid_stats.values() if v.get("words", 0) > 0)
        print(f"  photo_grid_stats.json: {len(_grid_stats)}/{len(_names)} manifest names matched, {_nz} with words>0")
        if _grid_stats and _nz == 0:
            raise RuntimeError(
                "photo_grid_stats: all 0 words — likely an annual_words key-type mismatch "
                "(in-memory keys are int, JSON keys are str). Don't ship this build."
            )
    except FileNotFoundError:
        pass

    # Top 25 wordiest (30+ articles, excluding Opinion/Magazine)
    excluded = {"Opinion", "Magazine", "T Magazine"}
    wordiest = [a for a in authors
                if a["article_count"] >= 30 and a["primary_section"] not in excluded]
    wordiest.sort(key=lambda a: a["avg_words"], reverse=True)
    wordiest = wordiest[:25]

    # --- World coverage: glocations by year ---
    # Merge city-level tags into their parent country, and fix all-caps names.
    # Contested/ambiguous geographies (Gaza Strip, West Bank, Taiwan, Hong Kong, etc.) are left as-is.
    # Location normalization is module-scope (see _normalize_loc above).

    # Countries where blog inflation was significant — pre-compute blog/non-blog split
    BLOG_SPLIT_COUNTRIES = {"India", "China", "Hong Kong"}

    world_articles = [a for a in articles if a["section"] == "World"]
    glocation_year = defaultdict(lambda: defaultdict(int))
    glocation_blog_year = defaultdict(lambda: defaultdict(int))
    glocation_total = Counter()
    region_year = defaultdict(lambda: defaultdict(int))

    for art in world_articles:
        y = str(art["year"])
        url = art.get("web_url", "") or ""
        is_blog = "blogs.nytimes.com" in url
        for loc in art.get("glocations", []):
            loc = _normalize_loc(loc)
            if loc is None:
                continue
            glocation_year[loc][y] += 1
            glocation_total[loc] += 1
            if is_blog and loc in BLOG_SPLIT_COUNTRIES:
                glocation_blog_year[loc][y] += 1
        sub = art.get("subsection", "")
        if sub:
            region_year[sub][y] += 1

    # All locations with >= 5 total articles (exclude trivial noise)
    top_locations = [loc for loc, cnt in glocation_total.most_common() if cnt >= 5]
    world_coverage = {
        "locations": top_locations,
        "location_trends": {loc: dict(glocation_year[loc]) for loc in top_locations},
        "blog_location_trends": {loc: dict(glocation_blog_year[loc]) for loc in BLOG_SPLIT_COUNTRIES},
        "region_trends": {r: dict(region_year[r]) for r in sorted(region_year.keys())},
        "years": all_years,
    }

    # --- US State coverage: use canonical_states pre-computed per article ---
    state_year = defaultdict(lambda: defaultdict(int))
    state_total = Counter()
    ny_state_year = defaultdict(lambda: defaultdict(int))
    ny_state_total = Counter()

    for art in articles:
        y = str(art["year"])
        if art["section"] == "U.S.":
            for state in art.get("canonical_states", []):
                state_year[state][y] += 1
                state_total[state] += 1
        elif art["section"] == "New York":
            for state in art.get("canonical_states", []):
                ny_state_year[state][y] += 1
                ny_state_total[state] += 1

    top_states = [s for s, _ in state_total.most_common()]
    # All states that appear in either section
    all_states_combined = sorted(set(list(state_total.keys()) + list(ny_state_total.keys())))
    us_state_coverage = {
        "states": top_states,
        "state_trends": {s: dict(state_year[s]) for s in top_states},
        "ny_state_trends": {s: dict(ny_state_year[s]) for s in ny_state_total.keys()},
        "ny_state_totals": dict(ny_state_total),
        "years": all_years,
    }

    # --- Features: recurring columns / special coverage (not standard sections) ---
    weddings_by_year = defaultdict(int)
    vows_col_by_year = defaultdict(int)
    weddings_authors = Counter()
    vows_col_authors = Counter()

    def _is_wedding_announcement(art):
        """Wedding announcement / Vows column filter.

        Pre-2021: NYT tagged these with subsection="Weddings".
        2021+: subsection went empty; announcements moved to /style/*-wedding.html
        and carry the "Weddings and Engagements" keyword. Pairing URL+keyword
        avoids sweeping in generic trend/analysis pieces that share the keyword.

        Vows column ID (historically): the "Vows (Times Column)" subject tag
        existed only 2011-2017. Before and after, the only reliable signal is
        the kicker. The kicker has read "Vows" consistently 2006-present, and
        "WEDDINGS: VOWS" / "WEDDINGS/CELEBRATIONS: VOWS" 2000-2005. Exclude
        "Mini-Vows" (a separate, lighter feature that emerged in 2017).
        """
        url_lc = (art.get("web_url", "") or "").lower()
        ss = (art.get("subsection", "") or "").lower()
        sb_lc = [s.lower() for s in (art.get("subjects", []) or [])]
        kicker_lc = (art.get("kicker", "") or "").strip().lower()
        is_mini = "mini" in kicker_lc  # 'mini-vows', 'mini vows', variants
        is_vows_col = (not is_mini) and (
            "vows (times column)" in sb_lc
            or kicker_lc == "vows"
            or kicker_lc.endswith(": vows")  # 2000-05: 'weddings: vows', 'weddings/celebrations: vows'
        )
        is_new_announcement = (
            url_lc.endswith("-wedding.html")
            and "weddings and engagements" in sb_lc
        )
        return ss == "weddings" or is_vows_col or is_new_announcement, is_vows_col

    for art in articles:
        is_weddings, is_vows_col = _is_wedding_announcement(art)
        y = str(art["year"])
        if is_weddings:
            weddings_by_year[y] += 1
            if not is_vows_col:
                for auth in art.get("authors", []):
                    if auth:
                        weddings_authors[auth] += 1
        if is_vows_col:
            vows_col_by_year[y] += 1
            for auth in art.get("authors", []):
                if auth:
                    vows_col_authors[auth] += 1

    # Collect all wedding/Vows articles for the popup article list
    URL_PREFIX_FULL = "https://www.nytimes.com"
    recent_wedding_articles = []
    for art in articles:
        url = art.get("web_url", "") or ""
        is_wed, _ = _is_wedding_announcement(art)
        if is_wed:
            u = url
            if u.startswith(URL_PREFIX_FULL):
                u = u[len(URL_PREFIX_FULL):]
            recent_wedding_articles.append({
                "d": art["pub_date"][:10],
                "h": art.get("headline", ""),
                "a": art.get("authors", []),
                "w": art.get("word_count", 0),
                "u": u,
            })
    # Stratified sample: up to 16 articles per year, sorted by date ascending.
    # This ensures newest/oldest toggle spans the full archive rather than just recent years.
    _wed_by_year = defaultdict(list)
    for art in recent_wedding_articles:
        _wed_by_year[art["d"][:4]].append(art)
    _sample = []
    for _yr in sorted(_wed_by_year.keys()):
        _sample.extend(sorted(_wed_by_year[_yr], key=lambda x: x["d"])[:16])
    recent_wedding_articles = _sample

    # Merge author lists (announcements + Vows column together)
    all_wed_authors = Counter()
    for n, c in weddings_authors.items():
        all_wed_authors[n] += c
    for n, c in vows_col_authors.items():
        all_wed_authors[n] += c

    # --- Letter of Recommendation feature (Magazine column, 2015–present) ---
    # Identified by headline prefix ("Letter of Recommendation: X") through ~2020,
    # then by kicker field ("Letter of Recommendation") from 2021 onward.
    lor_by_year = defaultdict(int)
    lor_authors = Counter()
    recent_lor_articles = []
    for art in articles:
        h = art.get("headline", "") or ""
        kicker = art.get("kicker", "") or ""
        is_lor = (
            h.lower().startswith("letter of recommendation:") or
            kicker.lower() == "letter of recommendation"
        )
        if not is_lor:
            continue
        y = str(art["year"])
        lor_by_year[y] += 1
        for auth in art.get("authors", []):
            lor_authors[auth] += 1
        url = art.get("web_url", "") or ""
        if url.startswith(URL_PREFIX_FULL):
            url = url[len(URL_PREFIX_FULL):]
        # Normalize display headline: prefix with "Letter of Recommendation: " if kicker-only
        display_h = h if h.lower().startswith("letter of recommendation:") else f"Letter of Recommendation: {h}"
        recent_lor_articles.append({
            "d": art["pub_date"][:10],
            "h": display_h,
            "a": art.get("authors", []),
            "w": art.get("word_count", 0),
            "u": url,
        })
    recent_lor_articles.sort(key=lambda x: x["d"], reverse=True)

    # --- Quote of the Day ---
    qotd_by_year = defaultdict(int)
    recent_qotd_articles = []
    for art in articles:
        h = art.get("headline", "") or ""
        tom = art.get("type_of_material", "") or ""
        sec = art.get("section", "") or ""
        is_qotd = (
            h.lower().startswith("quote of the day")
            or (tom == "Quote" and sec in ("Corrections", "Today's Paper"))
        )
        if not is_qotd:
            continue
        y = str(art["year"])
        qotd_by_year[y] += 1
        url = art.get("web_url", "") or ""
        if url.startswith(URL_PREFIX_FULL):
            url = url[len(URL_PREFIX_FULL):]
        recent_qotd_articles.append({
            "d": art["pub_date"][:10],
            "h": h,
            "a": art.get("authors", []),
            "w": art.get("word_count", 0),
            "u": url,
        })
    recent_qotd_articles.sort(key=lambda x: x["d"], reverse=True)

    features_data = {
        "weddings": {
            "by_year": dict(weddings_by_year),
            "vows_col_by_year": dict(vows_col_by_year),
            "top_authors": [{"name": n, "count": c} for n, c in all_wed_authors.most_common(15)],
            "recent_articles": recent_wedding_articles,
            "total": sum(weddings_by_year.values()),
        },
        "letter_of_recommendation": {
            "by_year": dict(lor_by_year),
            "top_authors": [{"name": n, "count": c} for n, c in lor_authors.most_common(15)],
            "recent_articles": recent_lor_articles,
            "total": sum(lor_by_year.values()),
        },
        "quote_of_the_day": {
            "by_year": dict(qotd_by_year),
            "recent_articles": recent_qotd_articles[:50],
            "total": sum(qotd_by_year.values()),
        },
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
    # Authors with >= 2 articles (matches authors.json export; excludes one-off contributors)
    total_authors = len([a for a in authors if a["article_count"] >= 2])
    authors_25plus = len([a for a in authors if a["article_count"] >= 25])
    unique_sections = len(set(a["section"] for a in articles if a["section"]))
    date_range = f"{months_sorted[0]} to {months_sorted[-1]}" if months_sorted else ""

    return {
        "build_date": datetime.now().strftime("%B %-d, %Y"),
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
        "sections": [all_sections_entry] + sections,
        "section_trends": section_trends,
        "all_years": all_years,
        "top_authors": top_authors,
        "wordiest_authors": wordiest,
        "multi_byline_trend": multi_byline_trend,
        "world_coverage": world_coverage,
        "us_state_coverage": us_state_coverage,
        "features": features_data,
    }


# Subject keyword continuity merges — NYT changed tag names over time,
# causing abrupt gaps in beat coverage. Map old → new canonical form.
_SUBJECT_KW_MERGES = {
    # Housing
    'Housing': 'Real Estate and Housing (Residential)',
    # Weapons/defense
    'ATOMIC WEAPONS': 'Nuclear Weapons',
    'UNITED STATES ARMAMENT AND DEFENSE': 'Armament, Defense and Military Forces',
    # Media/recordings
    'RECORDINGS (AUDIO)': 'Recordings and Downloads (Audio)',
    'RECORDINGS (VIDEO)': 'Recordings and Downloads (Video)',
    # Retail/fashion
    'APPAREL': 'Fashion and Apparel',
    'RETAIL STORES AND TRADE': 'Shopping and Retail',
    # Labor/immigration
    'LABOR': 'Labor and Jobs',
    'IMMIGRATION AND REFUGEES': 'Immigration and Emigration',
    # Advertising
    'ADVERTISING': 'Advertising and Marketing',
    # Social issues — old ALLCAPS/abbrev forms to current descriptive forms
    'Children and Youth': 'Children and Childhood',
    'Demonstrations and Riots': 'Demonstrations, Protests and Riots',
    'Demonstrations, Protests, and Riots': 'Demonstrations, Protests and Riots',  # comma variant
    'Murders and Attempted Murders': 'Murders, Attempted Murders and Homicides',
    'Education and Schools': 'Education (K-12)',
    'Banks and Banking': 'Banking and Financial Institutions',
    'Freedom and Human Rights': 'Human Rights and Human Rights Violations',
    'Suspensions, Dismissals and Resignations': 'Dismissals, Suspensions and Resignations',
}

_ORG_KW_MERGES = {
    'NEW YORK KNICKERBOCKERS': 'New York Knicks',
    # Company renames — merge old name into current
    'Facebook Inc': 'Meta Platforms Inc',   # renamed Oct 2021
    'Facebook.com': 'Meta Platforms Inc',
    # All-caps → mixed-case merges (same normalization as subjects)
    # Applied at ingestion so subjects.json stays consistent
}

def _normalize_subject_kw(name):
    """Merge discontinued NYT subject tags to their current equivalents.
    Also title-cases ALL-CAPS tags so e.g. 'ADVERTISING AND MARKETING'
    (all-caps variant) maps to the same form as 'Advertising and Marketing'.
    """
    if name in _SUBJECT_KW_MERGES:
        return _SUBJECT_KW_MERGES[name]
    # Title-case all-caps tags, then check merges again for variants
    alpha = [c for c in name if c.isalpha()]
    if alpha and all(c.isupper() for c in alpha) and (' ' in name or ',' in name):
        title = name.title()
        # Fix common prepositions that title() capitalizes incorrectly
        for word in (' And ', ' Or ', ' The ', ' Of ', ' In ', ' For ', ' To ', ' A '):
            title = title.replace(word, word.lower())
        return title
    return name

def _normalize_org_kw(name):
    """Merge discontinued NYT organization tags to their current equivalents."""
    if name in _ORG_KW_MERGES:
        return _ORG_KW_MERGES[name]
    return name

def _normalize_subject_name(name):
    """Title-case ALL CAPS names; normalize known capitalization variants."""
    # Known capitalization inconsistencies (e.g. 'Amazon.Com Inc' vs 'Amazon.com Inc')
    CANONICAL = {
        'amazon.com inc': 'Amazon.com Inc',
        'meta platforms inc': 'Meta Platforms Inc',
        'alphabet inc': 'Alphabet Inc',
    }
    lower = name.lower()
    if lower in CANONICAL:
        return CANONICAL[lower]
    alpha = [c for c in name if c.isalpha()]
    if alpha and all(c.isupper() for c in alpha) and (' ' in name or ',' in name):
        return name.title()
    return name


def build_subjects_data(articles):
    """Build persons and organizations keyword frequency data."""
    currentYear = datetime.now().year

    # Count per subject per year
    persons_annual = defaultdict(lambda: defaultdict(int))
    orgs_annual = defaultdict(lambda: defaultdict(int))

    for art in articles:
        year = str(art["year"])
        if art["year"] == currentYear:
            continue
        for p in art.get("persons", []):
            persons_annual[_normalize_subject_name(p)][year] += 1
        for o in art.get("organizations", []):
            orgs_annual[_normalize_subject_name(o)][year] += 1

    MIN_COUNT = 15  # filter rare entries

    # Find last year with meaningful persons/orgs keyword coverage
    # (NYT stopped tagging persons/orgs keywords in 2025)
    year_totals = defaultdict(int)
    for by_year in list(persons_annual.values()) + list(orgs_annual.values()):
        for y, c in by_year.items():
            year_totals[y] += c
    all_data_years = sorted(year_totals.keys())
    last_year = all_data_years[-1] if all_data_years else str(currentYear - 1)

    GENERATION_SUFFIXES = {"Jr", "Sr", "II", "III", "IV", "V", "2nd", "3rd"}

    def merge_person_variants(entries):
        """Merge 'Last, First' into 'Last, First Middle' when they're clearly the same person."""
        by_last = defaultdict(list)
        for e in entries:
            parts = e["name"].split(",", 1)
            if len(parts) == 2:
                by_last[parts[0].strip()].append(e)

        absorbed = set()
        for last, group in by_last.items():
            for i, e1 in enumerate(group):
                first1 = e1["name"].split(",", 1)[1].strip() if "," in e1["name"] else ""
                for e2 in group[i+1:]:
                    first2 = e2["name"].split(",", 1)[1].strip() if "," in e2["name"] else ""
                    # Determine which is shorter (simpler) name
                    if len(first1) < len(first2):
                        short_e, long_e, short_f, long_f = e1, e2, first1, first2
                    else:
                        short_e, long_e, short_f, long_f = e2, e1, first2, first1
                    # Must be a prefix match at word boundary
                    if not long_f.startswith(short_f + " "):
                        continue
                    # Skip generation suffixes (Jr/Sr means different person)
                    if long_f.split()[-1] in GENERATION_SUFFIXES:
                        continue
                    # Merge only when short name is clearly the rare variant
                    if short_e["total"] <= 50 and long_e["total"] >= short_e["total"] * 15:
                        # Absorb short into long
                        for yr, cnt in short_e["annual"].items():
                            long_e["annual"][yr] = long_e["annual"].get(yr, 0) + cnt
                        long_e["total"] += short_e["total"]
                        absorbed.add(short_e["name"])

        return [e for e in entries if e["name"] not in absorbed]

    def make_entries(annual_dict, is_persons=False):
        result = []
        for name, by_year in annual_dict.items():
            total = sum(by_year.values())
            if total < MIN_COUNT:
                continue
            result.append({"name": name, "total": total, "annual": dict(by_year)})
        result.sort(key=lambda x: x["total"], reverse=True)
        if is_persons:
            result = merge_person_variants(result)
            result.sort(key=lambda x: x["total"], reverse=True)
        return result

    return {
        "persons": make_entries(persons_annual, is_persons=True),
        "organizations": make_entries(orgs_annual),
        "last_year": last_year,
    }


def main():
    raw = load_all_articles()
    articles = process_articles(raw)

    print("Deduplicating articles...")
    articles, n_dupes = deduplicate_articles(articles)

    authors = build_author_stats(articles)

    print("Building beats data...")
    beats_json, author_beats_map = build_beats(articles, authors)
    for a in authors:
        a['beats'] = author_beats_map.get(a['name'], [])
    print(f"  {len(beats_json['subjectList'])} unique beats subjects")

    dashboard = build_dashboard_data(articles, authors)
    dashboard["summary"]["duplicates_removed"] = n_dupes

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
            # Store raw glocations for sublocation detection (city/region parentheticals)
            rec["g"] = a["glocations"]
            # Also store normalized top-level names for country/state matching
            normed = list(dict.fromkeys(_normalize_loc(g) for g in a["glocations"]))
            if normed != a["glocations"]:  # only add if normalization changed anything
                rec["gn"] = normed
        if a.get("canonical_states"):
            rec["st"] = a["canonical_states"]  # canonical US state names
        if a.get("subsection"):
            rec["ss"] = a["subsection"]  # subsection
        if a.get("subjects"):
            rec["sb"] = [_normalize_subject_kw(s) for s in a["subjects"]]  # subject keywords
        if a.get("persons"):
            rec["pe"] = a["persons"]   # persons keywords
        if a.get("organizations"):
            rec["og"] = [_normalize_org_kw(o) for o in a["organizations"]]  # organizations keywords
        if a.get("print_headline"):
            rec["ph"] = a["print_headline"]  # print headline (omit if empty to save space)
        if a.get("kicker"):
            rec["k"] = a["kicker"]           # kicker (for feature detection)
        by_year[a["year"]].append(rec)

    years = sorted(by_year.keys())
    for year in years:
        filepath = os.path.join(DATA_DIR, f"articles_{year}.json")
        with open(filepath, "w") as f:
            json.dump(by_year[year], f, separators=(',', ':'))
    print(f"Saved article files for {len(years)} years ({sum(len(v) for v in by_year.values()):,} articles)")

    # Write lean tracker files (headline+month only, ~28% the size of full articles)
    # Loaded first by the browser so the Headline Frequency tab is usable sooner.
    for year in years:
        tracker = []
        for rec in by_year[year]:
            t = {"h": rec["h"], "m": rec["m"]}
            if rec.get("ph"):
                t["ph"] = rec["ph"]
            if rec.get("ps") == "A" and str(rec.get("pp", "")) == "1":
                t["fp"] = 1
            tracker.append(t)
        fpath = os.path.join(DATA_DIR, f"tracker_{year}.json")
        with open(fpath, "w") as f:
            json.dump(tracker, f, separators=(',', ':'))
    print(f"Saved tracker files for {len(years)} years")

    # Only export authors with >= 2 articles; exclude institutional bylines
    authors_export = [
        a for a in authors
        if a["article_count"] >= 2 and a["name"] not in _INSTITUTIONAL_BYLINES
    ]
    with open(os.path.join(DATA_DIR, "authors.json"), "w") as f:
        json.dump(authors_export, f, separators=(',', ':'))
    print(f"Saved authors.json ({len(authors_export):,} authors, >= 2 articles)")

    with open(os.path.join(DATA_DIR, "dashboard.json"), "w") as f:
        json.dump(dashboard, f, separators=(',', ':'))
    print(f"Saved dashboard.json")

    with open(os.path.join(DATA_DIR, "beats.json"), "w") as f:
        json.dump(beats_json, f, separators=(',', ':'))
    print(f"Saved beats.json ({len(beats_json['subjectList'])} subjects)")

    print("Building subjects data...")
    subjects_data = build_subjects_data(articles)
    with open(os.path.join(DATA_DIR, "subjects.json"), "w") as f:
        json.dump(subjects_data, f, separators=(',', ':'))
    print(f"Saved subjects.json ({len(subjects_data['persons'])} persons, {len(subjects_data['organizations'])} organizations)")

    # --- US states GeoJSON for choropleth map ---
    shp_path = os.path.join(DATA_DIR, "cb_2023_us_state_20m.shp")
    geojson_path = os.path.join(DATA_DIR, "us_states.geojson")
    if os.path.exists(shp_path):
        try:
            import geopandas as gpd
            gdf = gpd.read_file(shp_path).to_crs("EPSG:4326")[["NAME", "STUSPS", "geometry"]]
            gdf["geometry"] = gdf["geometry"].simplify(0.01)
            with open(geojson_path, "w") as f:
                f.write(gdf.to_json(drop_id=True))
            print(f"Saved us_states.geojson ({os.path.getsize(geojson_path):,} bytes)")
        except ImportError:
            print("Skipping us_states.geojson (geopandas not installed)")


if __name__ == "__main__":
    main()
