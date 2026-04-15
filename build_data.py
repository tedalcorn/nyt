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
        if name[0] in '!-(\'&<':
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
            # Defunct/absorbed sections merged into closest surviving equivalent
            "Great Homes & Destinations": "Real Estate",  # luxury RE supplement 2002-2014
            "At Home": "Style",                           # COVID-era home-life section 2020-2022
            "Critic's Choice": "Arts",                    # arts picks feature folded into Arts
        }
        section = SECTION_MERGES.get(section, section)

        # Override section for obituaries that were filed under subject sections
        # (2011-2015: NYT tagged type_of_material='Obituary (Obit)' but put
        #  articles in Arts/Sports/Business/etc. rather than 'Obituaries')
        if mat == "Obituary (Obit)" and section != "Obituaries":
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
                subjects.append(SUBJECT_RENAMES.get(kw["value"], kw["value"]))
            elif kw_name in ("persons", "Persons"):
                persons_kw.append(kw["value"])
            elif kw_name in ("organizations", "Organizations"):
                organizations_kw.append(kw["value"])

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
            "persons": persons_kw,
            "organizations": organizations_kw,
            "canonical_states": canonical_states,
        })

    print(f"  {len(articles):,} processed, {skipped} skipped")

    # Manual overrides for names the NYT API consistently truncates or misspells.
    # Key: wrong form as it appears in the API data. Value: correct full name.
    AUTHOR_OVERRIDES = {
        # "St." compound last names — API drops the second word of the last name.
        # Only add entries here when the correct full name is confirmed.
        "Nicholas St":  "Nicholas St. Fleur",
        # Middle initial sometimes dropped / capitalization varies
        "Michael De La Merced": "Michael J. de la Merced",
        # Trailing "Photographs" suffix (byline parsed as "Name; Photographs by ...")
        "Ken Belson Photographs":   "Ken Belson",
        "Ilana Kaplan Photographs": "Ilana Kaplan",
        "Sarah Bahr Photographs":   "Sarah Bahr",
        # "X Nyt" suffix — Metro Briefing and wire-style bylines (2001-2006)
        'Abby Goodnough Nyt': 'Abby Goodnough',
        'Abby Gruen Nyt': 'Abby Gruen',
        'Abeer Allam Nyt': 'Abeer Allam',
        'Adam Clymer Nyt': 'Adam Clymer',
        'Adam Liptak Nyt': 'Adam Liptak',
        'Adam Nagourney Nyt': 'Adam Nagourney',
        'Adam Nossiter Nyt': 'Adam Nossiter',
        'Adrienne Lu Nyt': 'Adrienne Lu',
        "Ainsley O'connell Nyt": "Ainsley O'connell",
        'Al Baker Nyt': 'Al Baker',
        'Alan Cowell Nyt': 'Alan Cowell',
        'Alan Feuer Nyt': 'Alan Feuer',
        'Alan Finder Nyt': 'Alan Finder',
        'Alan Riding Nyt': 'Alan Riding',
        'Albert Salvato Nyt': 'Albert Salvato',
        'Alejandro Lazo Nyt': 'Alejandro Lazo',
        'Alessandra Stanley Nyt': 'Alessandra Stanley',
        'Alex Berenson Nyt': 'Alex Berenson',
        'Alex Kuczynski Nyt': 'Alex Kuczynski',
        'Alex Mindlin Nyt': 'Alex Mindlin',
        'Alexander Nurnberg Nyt': 'Alexander Nurnberg',
        'Alexandra Walsh Nyt': 'Alexandra Walsh',
        'Alicia Zubikowski Nyt': 'Alicia Zubikowski',
        'Alison Langley Nyt': 'Alison Langley',
        'Alison Mitchell Nyt': 'Alison Mitchell',
        'Alison Smale Nyt': 'Alison Smale',
        'Allison Fass Nyt': 'Allison Fass',
        'Amanda Hesser Nyt': 'Amanda Hesser',
        'Amelia Gentleman Nyt': 'Amelia Gentleman',
        'Amy Green Nyt': 'Amy Green',
        'Amy Harmon Nyt': 'Amy Harmon',
        'Amy Waldman Nyt': 'Amy Waldman',
        "Anahad O'connor Nyt": "Anahad O'Connor",
        'Anand Giridharadas Nyt': 'Anand Giridharadas',
        'Andrea Elliott Nyt': 'Andrea Elliott',
        'Andrew Jacobs Nyt': 'Andrew Jacobs',
        'Andrew Kramer Nyt': 'Andrew Kramer',
        'Andrew Pollack Nyt': 'Andrew Pollack',
        'Andrew Revkin Nyt': 'Andrew Revkin',
        'Andrew Salmon Nyt': 'Andrew Salmon',
        'Andrew Tangel Nyt': 'Andrew Tangel',
        'Andrew Zipern Nyt': 'Andrew Zipern',
        'Andy Jacobs Nyt': 'Andy Jacobs',
        'Andy Newman Nyt': 'Andy Newman',
        'Anemona Hartocollis Nyt': 'Anemona Hartocollis',
        'Ann Farmer Nyt': 'Ann Farmer',
        'Ann Wozencraft Nyt': 'Ann Wozencraft',
        'Anne Berryman Nyt': 'Anne Berryman',
        'Anne Raver Nyt': 'Anne Raver',
        'Anthee Carassava Nyt': 'Anthee Carassava',
        'Anthony Depalma Nyt': 'Anthony Depalma',
        'Anthony Ramirez Nyt': 'Anthony Ramirez',
        'Antonio Betancourt Nyt': 'Antonio Betancourt',
        'Ariane Bernard Nyt': 'Ariane Bernard',
        'Ariel Hart Nyt': 'Ariel Hart',
        'Avi Salzman Nyt': 'Avi Salzman',
        'Baradan Kuppusamy Nyt': 'Baradan Kuppusamy',
        'Barbara Crossette Nyt': 'Barbara Crossette',
        'Barbara Novovitch Nyt': 'Barbara Novovitch',
        'Barbara Stewart Nyt': 'Barbara Stewart',
        'Barbara Whitaker Nyt': 'Barbara Whitaker',
        'Barnaby Feder Nyt': 'Barnaby Feder',
        'Barney Feder Nyt': 'Barney Feder',
        'Barry Bearak Nyt': 'Barry Bearak',
        'Barry Meier Nyt': 'Barry Meier',
        'Becky Gaylord Nyt': 'Becky Gaylord',
        'Ben Bergman Nyt': 'Ben Bergman',
        'Ben Lefebvre Nyt': 'Ben Lefebvre',
        'Ben Shpigel Nyt': 'Ben Shpigel',
        'Benedict Carey Nyt': 'Benedict Carey',
        'Benjamin Jones Nyt': 'Benjamin Jones',
        'Benjamin Weiser Nyt': 'Benjamin Weiser',
        'Bernard Simon Nyt': 'Bernard Simon',
        'Bernard Weinraub Nyt': 'Bernard Weinraub',
        'Bernie Beglane Nyt': 'Bernie Beglane',
        'Bill Carter Nyt': 'Bill Carter',
        'Bill Dawson Nyt': 'Bill Dawson',
        'Bill Dedman Nyt': 'Bill Dedman',
        'Bill Finley Nyt': 'Bill Finley',
        'Bill Pennington Nyt': 'Bill Pennington',
        'Birgit Brauer Nyt': 'Birgit Brauer',
        'Blaine Harden Nyt': 'Blaine Harden',
        'Bob Tedeschi Nyt': 'Bob Tedeschi',
        'Borzou Daragahi Nyt': 'Borzou Daragahi',
        'Braden Phillips Nyt': 'Braden Phillips',
        'Brenda Goodman Nyt': 'Brenda Goodman',
        'Brian Alexander Nyt': 'Brian Alexander',
        'Brian Ellsworth Nyt': 'Brian Ellsworth',
        'Brian Knowlton Nyt': 'Brian Knowlton',
        'Brian Lavery Nyt': 'Brian Lavery',
        'Brian Wingfield Nyt': 'Brian Wingfield',
        'Bruce Lambert Nyt': 'Bruce Lambert',
        'Bud Norman Nyt': 'Bud Norman',
        'Caitlin Nish Nyt': 'Caitlin Nish',
        'Calvin Sims Nyt': 'Calvin Sims',
        'Campbell Robertson Nyt': 'Campbell Robertson',
        'Cara Buckley Nyt': 'Cara Buckley',
        'Carey Goldberg Nyt': 'Carey Goldberg',
        'Carl Hulse Nyt': 'Carl Hulse',
        'Carla Baranauckas Nyt': 'Carla Baranauckas',
        'Carla Bass Nyt': 'Carla Bass',
        'Carlo Piano Nyt': 'Carlo Piano',
        'Carlotta Gall Nyt': 'Carlotta Gall',
        'Carol Pogash Nyt': 'Carol Pogash',
        'Carol Vogel Nyt': 'Carol Vogel',
        'Carolyn Marshall Nyt': 'Carolyn Marshall',
        'Cassi Feldman Nyt': 'Cassi Feldman',
        'Catherine Billey Nyt': 'Catherine Billey',
        'Catherine Greenman Nyt': 'Catherine Greenman',
        'Celestine Bohlen Nyt': 'Celestine Bohlen',
        'Charles Bagli Nyt': 'Charles Bagli',
        'Charlie Leduff Nyt': 'Charlie Leduff',
        'Chris Buckley Nyt': 'Chris Buckley',
        'Chris Dixon Nyt': 'Chris Dixon',
        'Chris Gaither Nyt': 'Chris Gaither',
        'Chris Mason Nyt': 'Chris Mason',
        'Christine Haughney Nyt': 'Christine Haughney',
        'Christine Hauser Nyt': 'Christine Hauser',
        'Christine Whitehouse Nyt': 'Christine Whitehouse',
        'Christopher Drew Nyt': 'Christopher Drew',
        'Christopher Elliott Nyt': 'Christopher Elliott',
        'Christopher Maag Nyt': 'Christopher Maag',
        'Christopher Marquis Nyt': 'Christopher Marquis',
        'Christopher Mason Nyt': 'Christopher Mason',
        'Christopher Pala Nyt': 'Christopher Pala',
        'Cindy Chang Nyt': 'Cindy Chang',
        'Claire Hoffman Nyt': 'Claire Hoffman',
        'Clifford Krauss Nyt': 'Clifford Krauss',
        'Clifton Brown Nyt': 'Clifton Brown',
        'Clyde Haberman Nyt': 'Clyde Haberman',
        'Colin Campbell Nyt': 'Colin Campbell',
        'Colin Moynihan Nyt': 'Colin Moynihan',
        'Conrad Mulcahy Nyt': 'Conrad Mulcahy',
        'Corey Kilgannon Nyt': 'Corey Kilgannon',
        'Cornelia Dean Nyt': 'Cornelia Dean',
        'Craig Smith Nyt': 'Craig Smith',
        'Cybele Sack Nyt': 'Cybele Sack',
        'Daisy Hernandez Nyt': 'Daisy Hernandez',
        'Daisy Hernández Nyt': 'Daisy Hernández',
        'Dale Fuchs Nyt': 'Dale Fuchs',
        'Damien Cave Nyt': 'Damien Cave',
        'Damon Hack Nyt': 'Damon Hack',
        'Dan Barry Nyt': 'Dan Barry',
        'Dan Heyman Nyt': 'Dan Heyman',
        'Dana Bayerle Nyt': 'Dana Bayerle',
        'Dana Beyerle Nyt': 'Dana Beyerle',
        'Dana Byerele Nyt': 'Dana Byerele',
        'Dana Canedy Nyt': 'Dana Canedy',
        'Daniel Simpson Nyt': 'Daniel Simpson',
        'Danny Hakim Nyt': 'Danny Hakim',
        'Daryl Khan Nyt': 'Daryl Khan',
        'Dave Caldwell Nyt': 'Dave Caldwell',
        'David Barboza Nyt': 'David Barboza',
        'David Barstow Nyt': 'David Barstow',
        'David Bernstein Nyt': 'David Bernstein',
        'David Binder Nyt': 'David Binder',
        'David Carr Nyt': 'David Carr',
        'David Enders Nyt': 'David Enders',
        'David Firestone Nyt': 'David Firestone',
        'David Gonzalez Nyt': 'David Gonzalez',
        'David Halbfinger Nyt': 'David Halbfinger',
        'David Herszenhorn Nyt': 'David Herszenhorn',
        'David Johnston Nyt': 'David Johnston',
        'David Kirpatrick Nyt': 'David Kirpatrick',
        'David Kocienewski Nyt': 'David Kocienewski',
        'David Kocieniewski Nyt': 'David Kocieniewski',
        'David Montero Nyt': 'David Montero',
        'David Picker Nyt': 'David Picker',
        'David Rohde Nyt': 'David Rohde',
        'David Scharfenberg Nyt': 'David Scharfenberg',
        'David Staba Nyt': 'David Staba',
        'David Stout Nyt': 'David Stout',
        'David Winzelberg Nyt': 'David Winzelberg',
        'Dean Murphy Nyt': 'Dean Murphy',
        'Deborah Sontag Nyt': 'Deborah Sontag',
        'Debra West Nyt': 'Debra West',
        'Denise Grady Nyt': 'Denise Grady',
        'Dennis Blank Nyt': 'Dennis Blank',
        'Dennis Overbye Nyt': 'Dennis Overbye',
        'Denny Lee Nyt': 'Denny Lee',
        'Desmond Butler Nyt': 'Desmond Butler',
        'Dexter Filkins Nyt': 'Dexter Filkins',
        'Dhruba Adhikary Nyt': 'Dhruba Adhikary',
        'Dian Saputra Nyt': 'Dian Saputra',
        'Diane Cardwell Nyt': 'Diane Cardwell',
        'Dina Kraft Nyt': 'Dina Kraft',
        'Dinitia Smith Nyt': 'Dinitia Smith',
        'Don Kirk Nyt': 'Don Kirk',
        'Donald Kirk Nyt': 'Donald Kirk',
        'Doug Frantz Nyt': 'Doug Frantz',
        'Doug Mcinnis Nyt': 'Doug McInnis',
        'Douglas Frantz Nyt': 'Douglas Frantz',
        'Douglas Jehl Nyt': 'Douglas Jehl',
        'Dylan Mcclain Nyt': 'Dylan Mcclain',
        'Eamon Quinn Nyt': 'Eamon Quinn',
        'Ed Andrews Nyt': 'Ed Andrews',
        'Eddy Ramirez Nyt': 'Eddy Ramirez',
        'Eddy Ramírez Nyt': 'Eddy Ramírez',
        'Eduardo Castillo Nyt': 'Eduardo Castillo',
        'Edward Wong Nyt': 'Edward Wong',
        'Edward Wyatt Nyt': 'Edward Wyatt',
        'Elaine Sciolino Nyt': 'Elaine Sciolino',
        'Eli Sanders Nyt': 'Eli Sanders',
        'Elisabeth Becker Nyt': 'Elisabeth Becker',
        'Elisabeth Bumiller Nyt': 'Elisabeth Bumiller',
        'Elisabeth Malkin Nyt': 'Elisabeth Malkin',
        'Elisabeth Rosenthal Nyt': 'Elisabeth Rosenthal',
        'Elisabetta Povoledo Nyt': 'Elisabetta Povoledo',
        'Elissa Gootman Nyt': 'Elissa Gootman',
        'Elizabeth Ahlin Nyt': 'Elizabeth Ahlin',
        'Elizabeth Becker Nyt': 'Elizabeth Becker',
        'Elizabeth Nash Nyt': 'Elizabeth Nash',
        'Elizabeth Olsen Nyt': 'Elizabeth Olsen',
        'Elizabeth Olson Nyt': 'Elizabeth Olson',
        'Elizabeth Stanton Nyt': 'Elizabeth Stanton',
        'Eman Wahby Nyt': 'Eman Wahby',
        'Emily Vasquez Nyt': 'Emily Vasquez',
        'Emily Yellin Nyt': 'Emily Yellin',
        'Emma Daley Nyt': 'Emma Daley',
        'Emma Daly Nyt': 'Emma Daly',
        'Eric Dash Nyt': 'Eric Dash',
        'Eric Ferkenhoff Nyt': 'Eric Ferkenhoff',
        'Eric Lichtblau Nyt': 'Eric Lichtblau',
        'Eric Lipton Nyt': 'Eric Lipton',
        "Eric O'keefe Nyt": "Eric O'Keefe",
        'Eric Schmitt Nyt': 'Eric Schmitt',
        'Eric Sylver Nyt': 'Eric Sylver',
        'Eric Sylvers Nyt': 'Eric Sylvers',
        'Eric Wilson Nyt': 'Eric Wilson',
        'Erica Goode Nyt': 'Erica Goode',
        'Erik Eckholm Nyt': 'Erik Eckholm',
        'Erika Kinetz Nyt': 'Erika Kinetz',
        'Ernie Beglane Nyt': 'Ernie Beglane',
        'Ethan Wilensky-lanford Nyt': 'Ethan Wilensky-Lanford',
        'Evelyn Nieves Nyt': 'Evelyn Nieves',
        'Evelyn Rusli Nyt': 'Evelyn Rusli',
        'Faiza Akhtar Nyt': 'Faiza Akhtar',
        'Fatou Diakhaté Nyt': 'Fatou Diakhaté',
        'Felicity Barringer Nyt': 'Felicity Barringer',
        'Fernanda Santos Nyt': 'Fernanda Santos',
        'Fiona Fleck Nyt': 'Fiona Fleck',
        'Florence Fabricant Nyt': 'Florence Fabricant',
        'Floyd Norris Nyt': 'Floyd Norris',
        'Ford Burkhart Nyt': 'Ford Burkhart',
        'Ford Fessenden Nyt': 'Ford Fessenden',
        'Fox Butterfield Nyt': 'Fox Butterfield',
        'Frank Bruni Nyt': 'Frank Bruni',
        'Frank Litsky Nyt': 'Frank Litsky',
        'Gardiner Harris Nyt': 'Gardiner Harris',
        'Gary Fineout Nyt': 'Gary Fineout',
        'Gary Gately Nyt': 'Gary Gately',
        'Gary Rivlin Nyt': 'Gary Rivlin',
        'Geraldine Fabrikant Nyt': 'Geraldine Fabrikant',
        'Gina Kolata Nyt': 'Gina Kolata',
        'Ginger Thompson Nyt': 'Ginger Thompson',
        'Glen Justice Nyt': 'Glen Justice',
        'Glenn Collins Nyt': 'Glenn Collins',
        'Glenn Fleishman Nyt': 'Glenn Fleishman',
        'Glenn Justice Nyt': 'Glenn Justice',
        'Graham Bowley Nyt': 'Graham Bowley',
        'Graham Gori Nyt': 'Graham Gori',
        'Greg Myer Nyt': 'Greg Myer',
        'Greg Myre Nyt': 'Greg Myre',
        'Greg Retsinas Nyt': 'Greg Retsinas',
        'Greg Winter Nyt': 'Greg Winter',
        'Gregory Crouch Nyt': 'Gregory Crouch',
        'Gretchen Reuthling Nyt': 'Gretchen Reuthling',
        'Gretchen Ruethling Nyt': 'Gretchen Ruethling',
        'Gustav Niebuhr Nyt': 'Gustav Niebuhr',
        'Guy Trebay Nyt': 'Guy Trebay',
        'Hari Kumar Nyt': 'Hari Kumar',
        'Heather Stewart Nyt': 'Heather Stewart',
        'Heather Timmons Nyt': 'Heather Timmons',
        'Helene Cooper Nyt': 'Helene Cooper',
        'Helene Fouquet Nyt': 'Helene Fouquet',
        'Henri Cauvin Nyt': 'Henri Cauvin',
        'Hope Reeves Nyt': 'Hope Reeves',
        'Howard Beck Nyt': 'Howard Beck',
        'Howard French Nyt': 'Howard French',
        'Hugh Eakin Nyt': 'Hugh Eakin',
        'Hélène Fouquet Nyt': 'Hélène Fouquet',
        'Ian Austen Nyt': 'Ian Austen',
        'Ian Fisher Nyt': 'Ian Fisher',
        'Ian Urbina Nyt': 'Ian Urbina',
        'Ilan Greenberg Nyt': 'Ilan Greenberg',
        'Iver Peterson Nyt': 'Iver Peterson',
        'Jack Bell Nyt': 'Jack Bell',
        'Jack Curry Nyt': 'Jack Curry',
        'Jacob Fries Nyt': 'Jacob Fries',
        'Jacques Steinberg Nyt': 'Jacques Steinberg',
        'James Barron Nyt': 'James Barron',
        'James Brooke Nyt': 'James Brooke',
        'James Dao Nyt': 'James Dao',
        'James Glanz Nyt': 'James Glanz',
        'James Gorman Nyt': 'James Gorman',
        'James Risen Nyt': 'James Risen',
        'James Sterngold Nyt': 'James Sterngold',
        'James Willhite Nyt': 'James Willhite',
        'Jane Allande-hession Nyt': 'Jane Allande-Hession',
        'Jane Fritsch Nyt': 'Jane Fritsch',
        'Jane Levere Nyt': 'Jane Levere',
        'Jane Perlez Nyt': 'Jane Perlez',
        'Janny Scott Nyt': 'Janny Scott',
        'Janon Fisher Nyt': 'Janon Fisher',
        'Jason Begay Nyt': 'Jason Begay',
        'Jason Diamos Nyt': 'Jason Diamos',
        'Jason George Nyt': 'Jason George',
        'Jason Horowitz Nyt': 'Jason Horowitz',
        'Jayson Blair Nyt': 'Jayson Blair',
        'Jed Stevenson Nyt': 'Jed Stevenson',
        'Jeff Leeds Nyt': 'Jeff Leeds',
        'Jeff Zeleny Nyt': 'Jeff Zeleny',
        'Jeffrey Gettleman Nyt': 'Jeffrey Gettleman',
        'Jenna Payne Nyt': 'Jenna Payne',
        'Jennifer Dunning Nyt': 'Jennifer Dunning',
        'Jennifer Medina Nyt': 'Jennifer Medina',
        'Jennifer Rich Nyt': 'Jennifer Rich',
        'Jennifer Steinhauer Nyt': 'Jennifer Steinhauer',
        'Jennnifer Steinhauer Nyt': 'Jennnifer Steinhauer',
        'Jenny Hontz Nyt': 'Jenny Hontz',
        'Jenny Medina Nyt': 'Jenny Medina',
        'Jeremy Peters Nyt': 'Jeremy Peters',
        'Jess Wisloski Nyt': 'Jess Wisloski',
        'Jesse Mckinley Nyt': 'Jesse McKinley',
        'Jessica Bruder Nyt': 'Jessica Bruder',
        'Jim Dwyer Nyt': 'Jim Dwyer',
        'Jim Noles Nyt': 'Jim Noles',
        "Jim O'grady Nyt": "Jim O'Grady",
        'Jim Robbins Nyt': 'Jim Robbins',
        'Jim Rutenberg Nyt': 'Jim Rutenberg',
        'Jim Yardley Nyt': 'Jim Yardley',
        'Jo Napolitano Nyt': 'Jo Napolitano',
        'Jo Thomas Nyt': 'Jo Thomas',
        'Jodi Rudoren Nyt': 'Jodi Rudoren',
        'Jodi Wilgoren Nyt': 'Jodi Wilgoren',
        'Joe Brescia Nyt': 'Joe Brescia',
        'Joe Drape Nyt': 'Joe Drape',
        'Joe Follick Nyt': 'Joe Follick',
        'Joe Sharkey Nyt': 'Joe Sharkey',
        'Joe Ward Nyt': 'Joe Ward',
        'Joel Brinkley Nyt': 'Joel Brinkley',
        'Joel Greenberg Nyt': 'Joel Greenberg',
        'Johanna Jainchill Nyt': 'Johanna Jainchill',
        'John Biggs Nyt': 'John Biggs',
        'John Branch Nyt': 'John Branch',
        'John Branston Nyt': 'John Branston',
        'John Broder Nyt': 'John Broder',
        'John Carpenter Nyt': 'John Carpenter',
        'John Desantis Nyt': 'John Desantis',
        'John Eligon Nyt': 'John Eligon',
        'John Files Nyt': 'John Files',
        'John Harney Nyt': 'John Harney',
        'John Holl Nyt': 'John Holl',
        'John Holusha Nyt': 'John Holusha',
        'John Kifner Nyt': 'John Kifner',
        'John Markoff Nyt': 'John Markoff',
        'John Moody Nyt': 'John Moody',
        'John Rather Nyt': 'John Rather',
        'John Schwartz Nyt': 'John Schwartz',
        'John Shaw Nyt': 'John Shaw',
        'John Sullivan Nyt': 'John Sullivan',
        'John Tagliabue Nyt': 'John Tagliabue',
        'Jon Pareles Nyt': 'Jon Pareles',
        'Jonathan Fuerbringer Nyt': 'Jonathan Fuerbringer',
        'Jonathan Glater Nyt': 'Jonathan Glater',
        'Jonathan Hicks Nyt': 'Jonathan Hicks',
        'Jonathan Marino Nyt': 'Jonathan Marino',
        'Jonathan Miller Nyt': 'Jonathan Miller',
        'Joseph Berger Nyt': 'Joseph Berger',
        'Joseph Kahn Nyt': 'Joseph Kahn',
        'Joseph Kolb Nyt': 'Joseph Kolb',
        'Joseph Treaster Nyt': 'Joseph Treaster',
        'Josh Barbanel Nyt': 'Josh Barbanel',
        'Josh Benson Nyt': 'Josh Benson',
        'José Ramírez Nyt': 'José Ramírez',
        'Joya Rajadhyaksha Nyt': 'Joya Rajadhyaksha',
        'Joyce Wadler Nyt': 'Joyce Wadler',
        'Juan Forero Nyt': 'Juan Forero',
        'Judith Berck Nyt': 'Judith Berck',
        'Judith Miller Nyt': 'Judith Miller',
        'Judy Battista Nyt': 'Judy Battista',
        'Judy Berck Nyt': 'Judy Berck',
        'Julia Mead Nyt': 'Julia Mead',
        'Julia Moskin Nyt': 'Julia Moskin',
        'Julia Preston Nyt': 'Julia Preston',
        'Julie Bosman Nyt': 'Julie Bosman',
        'Julie Dunn Nyt': 'Julie Dunn',
        'Julie Flaherty Nyt': 'Julie Flaherty',
        'Juliet Macur Nyt': 'Juliet Macur',
        'Justo Casal Nyt': 'Justo Casal',
        'Kareem Fahim Nyt': 'Kareem Fahim',
        'Karen Arenson Nyt': 'Karen Arenson',
        'Karen Crouse Nyt': 'Karen Crouse',
        'Karen Demasters Nyt': 'Karen Demasters',
        'Kari Haskell Nyt': 'Kari Haskell',
        'Kate Hammer Nyt': 'Kate Hammer',
        'Kate Phillips Nyt': 'Kate Phillips',
        'Kate Zernike Nyt': 'Kate Zernike',
        'Katherine Boas Nyt': 'Katherine Boas',
        'Katherine Zezima Nyt': 'Katherine Zezima',
        'Katherine Zoepf Nyt': 'Katherine Zoepf',
        'Kathryn Shattuck Nyt': 'Kathryn Shattuck',
        'Katie Kelley Nyt': 'Katie Kelley',
        'Katie Zezima Nyt': 'Katie Zezima',
        'Katrin Bennhold Nyt': 'Katrin Bennhold',
        'Katy Reckdahl Nyt': 'Katy Reckdahl',
        'Keith Bradsher Nyt': 'Keith Bradsher',
        'Ken Belsen Nyt': 'Ken Belsen',
        'Ken Belson Nyt': 'Ken Belson',
        'Kenneth Chang Nyt': 'Kenneth Chang',
        'Kerri Shaw Nyt': 'Kerri Shaw',
        'Kerry Shaw Nyt': 'Kerry Shaw',
        'Kevin Flynn Nyt': 'Kevin Flynn',
        'Kevin Sack Nyt': 'Kevin Sack',
        'Kimberly Chase Nyt': 'Kimberly Chase',
        'Kirk Johnson Nyt': 'Kirk Johnson',
        'Kirk Semple Nyt': 'Kirk Semple',
        'Kirsten Grieshaber Nyt': 'Kirsten Grieshaber',
        'Larry Rohter Nyt': 'Larry Rohter',
        'Larry Zuckerman Nyt': 'Larry Zuckerman',
        'Laura Holson Nyt': 'Laura Holson',
        'Laura Lee Nyt': 'Laura Lee',
        'Laura Mansnerus Nyt': 'Laura Mansnerus',
        'Laurie Goodstein Nyt': 'Laurie Goodstein',
        'Lee Jenkins Nyt': 'Lee Jenkins',
        'Leena Saidi Nyt': 'Leena Saidi',
        'Leslie Eaton Nyt': 'Leslie Eaton',
        'Leslie Kaufman Nyt': 'Leslie Kaufman',
        'Lia Miller Nyt': 'Lia Miller',
        'Libby Sander Nyt': 'Libby Sander',
        'Lily Koppel Nyt': 'Lily Koppel',
        'Linda Greenhouse Nyt': 'Linda Greenhouse',
        'Lisa Bacon Nyt': 'Lisa Bacon',
        'Lisa Foderaro Nyt': 'Lisa Foderaro',
        'Lisa Guernsey Nyt': 'Lisa Guernsey',
        'Liz Robbins Nyt': 'Liz Robbins',
        'Lizette Alvarez Nyt': 'Lizette Alvarez',
        'Lloyd Dunkeberger Nyt': 'Lloyd Dunkeberger',
        'Lloyd Dunkelberger Nyt': 'Lloyd Dunkelberger',
        'Lola Ogunnaike Nyt': 'Lola Ogunnaike',
        'Louise Story Nyt': 'Louise Story',
        'Lydia Polgreen Nyt': 'Lydia Polgreen',
        'Lynette Clemetson Nyt': 'Lynette Clemetson',
        'Lynette Holloway Nyt': 'Lynette Holloway',
        'Lynn Waddell Nyt': 'Lynn Waddell',
        'Lynn Zinser Nyt': 'Lynn Zinser',
        'Lynnley Browning Nyt': 'Lynnley Browning',
        'Manny Fernandez Nyt': 'Manny Fernandez',
        'Marc Lacey Nyt': 'Marc Lacey',
        'Marc Santora Nyt': 'Marc Santora',
        'Marcin Skomial Nyt': 'Marcin Skomial',
        'Marcos Mocine-mcqueen Nyt': 'Marcos Mocine-McQueen',
        'Marek Fuchs Nyt': 'Marek Fuchs',
        'Maria Newman Nyt': 'Maria Newman',
        'Marian Burros Nyt': 'Marian Burros',
        'Marian Smith Nyt': 'Marian Smith',
        'Marina Harss Nyt': 'Marina Harss',
        'Marina Harssome Nyt': 'Marina Harssome',
        'Marjorie Connelly Nyt': 'Marjorie Connelly',
        'Mark Glassman Nyt': 'Mark Glassman',
        'Mark Landler Nyt': 'Mark Landler',
        'Marlise Simons Nyt': 'Marlise Simons',
        'Martin Fackler Nyt': 'Martin Fackler',
        'Martin Stolz Nyt': 'Martin Stolz',
        'Marty Katz Nyt': 'Marty Katz',
        'Mary Reinholz Nyt': 'Mary Reinholz',
        'Mary Spicuzza Nyt': 'Mary Spicuzza',
        'Matt Birkbeck Nyt': 'Matt Birkbeck',
        'Matt Richtel Nyt': 'Matt Richtel',
        'Matt Viser Nyt': 'Matt Viser',
        'Matthew Healey Nyt': 'Matthew Healey',
        'Matthew Preusch Nyt': 'Matthew Preusch',
        'Matthew Sweeney Nyt': 'Matthew Sweeney',
        'Maureen Balleza Nyt': 'Maureen Balleza',
        'Melena Ryzik Nyt': 'Melena Ryzik',
        'Melinda Henneberger Nyt': 'Melinda Henneberger',
        'Merri Rosenberg Nyt': 'Merri Rosenberg',
        'Mery Galanternick Nyt': 'Mery Galanternick',
        'Michael Amon Nyt': 'Michael Amon',
        'Michael Brick Nyt': 'Michael Brick',
        'Michael Cooper Nyt': 'Michael Cooper',
        'Michael Janofsky Nyt': 'Michael Janofsky',
        'Michael Kamber Nyt': 'Michael Kamber',
        'Michael Luo Nyt': 'Michael Luo',
        'Michael Mcintire Nyt': 'Michael McIntire',
        'Michael Moss Nyt': 'Michael Moss',
        'Michael Schwirtz Nyt': 'Michael Schwirtz',
        'Michael Slackman Nyt': 'Michael Slackman',
        'Michael Weinreb Nyt': 'Michael Weinreb',
        'Michael Wilson Nyt': 'Michael Wilson',
        'Michael Wines Nyt': 'Michael Wines',
        'Michele Kayal Nyt': 'Michele Kayal',
        'Micheline Maynard Nyt': 'Micheline Maynard',
        'Michelle Kayal Nyt': 'Michelle Kayal',
        "Michelle O'donnell Nyt": "Michelle O'Donnell",
        'Michelle York Nyt': 'Michelle York',
        "Michelleo'donnell Nyt": "Michelleo'donnell",
        'Mick Meenan Nyt': 'Mick Meenan',
        'Mike Mcintire Nyt': 'Mike McIntire',
        'Miki Tanikawa Nyt': 'Miki Tanikawa',
        'Milt Freudenheim Nyt': 'Milt Freudenheim',
        'Mindy Sink Nyt': 'Mindy Sink',
        'Mireya Navarro Nyt': 'Mireya Navarro',
        'Mirta Ojito Nyt': 'Mirta Ojito',
        'Mitch Abramson Nyt': 'Mitch Abramson',
        'Mohammad Khan Nyt': 'Mohammad Khan',
        'Mohammed Khan Nyt': 'Mohammed Khan',
        'Mona Al-naggar Nyt': 'Mona Al-naggar',
        'Mona El-naggar Nyt': 'Mona El-Naggar',
        'Monica Davey Nyt': 'Monica Davey',
        'Monica Potts Nyt': 'Monica Potts',
        'Monte Williams Nyt': 'Monte Williams',
        'Motoko Rich Nyt': 'Motoko Rich',
        'Murray Chass Nyt': 'Murray Chass',
        'Naila-jean Meyers Nyt': 'Naila-jean Meyers',
        'Nat Ives Nyt': 'Nat Ives',
        'Nate Schweber Nyt': 'Nate Schweber',
        'Nathaniel Vinton Nyt': 'Nathaniel Vinton',
        'Nazila Fathi Nyt': 'Nazila Fathi',
        'Neela Banerjee Nyt': 'Neela Banerjee',
        'Neil Lewis Nyt': 'Neil Lewis',
        'Neil Macfarquhar Nyt': 'Neil MacFarquhar',
        'Nicholas Confessore Nyt': 'Nicholas Confessore',
        'Nicholas Wade Nyt': 'Nicholas Wade',
        'Nicholas Wood Nyt': 'Nicholas Wood',
        'Nick Madigan Nyt': 'Nick Madigan',
        'Nicole Cotroneo Nyt': 'Nicole Cotroneo',
        'Nicole Itano Nyt': 'Nicole Itano',
        'Nina Bernstein Nyt': 'Nina Bernstein',
        'Nora Krug Nyt': 'Nora Krug',
        'Norimitsu Onishi Nyt': 'Norimitsu Onishi',
        'Oren Yaniv Nyt': 'Oren Yaniv',
        'Pam Belluck Nyt': 'Pam Belluck',
        'Pascale Bonnefoy Nyt': 'Pascale Bonnefoy',
        'Pat Borzi Nyt': 'Pat Borzi',
        'Patrick Healy Nyt': 'Patrick Healy',
        'Patrick Mcgeehan Nyt': 'Patrick McGeehan',
        'Paul Meller Nyt': 'Paul Meller',
        'Paul Vitello Nyt': 'Paul Vitello',
        'Paul Zielbauer Nyt': 'Paul Zielbauer',
        'Paulo Prada Nyt': 'Paulo Prada',
        'Pete Thamel Nyt': 'Pete Thamel',
        'Peter Beller Nyt': 'Peter Beller',
        'Peter Gelling Nyt': 'Peter Gelling',
        'Peter Kiefer Nyt': 'Peter Kiefer',
        'Petra Kappl Nyt': 'Petra Kappl',
        'Philip Shenon Nyt': 'Philip Shenon',
        'Rachel Metz Nyt': 'Rachel Metz',
        'Rachel Swarns Nyt': 'Rachel Swarns',
        'Rachel Thorner Nyt': 'Rachel Thorner',
        'Ralph Blumenthal Nyt': 'Ralph Blumenthal',
        'Randy Kennedy Nyt': 'Randy Kennedy',
        'Ray Glier Nyt': 'Ray Glier',
        'Ray Rivera Nyt': 'Ray Rivera',
        'Raymond Bonner Nyt': 'Raymond Bonner',
        'Raymond Hernandez Nyt': 'Raymond Hernandez',
        "Rebecca O'brien Nyt": "Rebecca O'brien",
        'Reed Abelson Nyt': 'Reed Abelson',
        'Regan Morris Nyt': 'Regan Morris',
        'Renwick Mcclean Nyt': 'Renwick Mcclean',
        'Renwick Mclean Nyt': 'Renwick McLean',
        'Rich Tucker Nyt': 'Rich Tucker',
        'Richard Bernstein Nyt': 'Richard Bernstein',
        'Richard Perez-pena Nyt': 'Richard Perez-Pena',
        'Richard Pérez-peña Nyt': 'Richard Pérez-Peña',
        'Richard Sandomir Nyt': 'Richard Sandomir',
        'Rick Lyman Nyt': 'Rick Lyman',
        'Rita Farrell Nyt': 'Rita Farrell',
        'Rob Gunnison Nyt': 'Rob Gunnison',
        'Robert Hanley Nyt': 'Robert Hanley',
        'Robert Pear Nyt': 'Robert Pear',
        'Robert Strauss Nyt': 'Robert Strauss',
        'Robert Worth Nyt': 'Robert Worth',
        'Robin Pogrebin Nyt': 'Robin Pogrebin',
        'Robin Shulman Nyt': 'Robin Shulman',
        'Roger Cohen Nyt': 'Roger Cohen',
        'Ronald Smothers Nyt': 'Ronald Smothers',
        'Ross Milloy Nyt': 'Ross Milloy',
        'Ruhullah Khapalwak Nyt': 'Ruhullah Khapalwak',
        'Ruthie Ackerman Nyt': 'Ruthie Ackerman',
        'Sabrina Tavernise Nyt': 'Sabrina Tavernise',
        'Salman Masood Nyt': 'Salman Masood',
        'Sam Dillon Nyt': 'Sam Dillon',
        'Sam Len Nyt': 'Sam Len',
        'Sam Roberts Nyt': 'Sam Roberts',
        'Samar Aboul-fotouh Nyt': 'Samar Aboul-Fotouh',
        'Samuel Abt Nyt': 'Samuel Abt',
        'Samuel Len Nyt': 'Samuel Len',
        'Sandra Blakeslee Nyt': 'Sandra Blakeslee',
        'Sandra Harwitt Nyt': 'Sandra Harwitt',
        'Sarah Garland Nyt': 'Sarah Garland',
        'Sarah Kershaw Nyt': 'Sarah Kershaw',
        'Sarah Lyall Nyt': 'Sarah Lyall',
        'Sarah Plass Nyt': 'Sarah Plass',
        'Saritha Rai Nyt': 'Saritha Rai',
        'Sasha Cavender Nyt': 'Sasha Cavender',
        'Saul Hansell Nyt': 'Saul Hansell',
        'Scott Shane Nyt': 'Scott Shane',
        'Scott Veale Nyt': 'Scott Veale',
        'Sebnem Arsu Nyt': 'Sebnem Arsu',
        'Sebnen Arsu Nyt': 'Sebnen Arsu',
        'Serge Schmemann Nyt': 'Serge Schmemann',
        'Seth Mydans Nyt': 'Seth Mydans',
        'Seth Mydens Nyt': 'Seth Mydens',
        'Seth Schiesel Nyt': 'Seth Schiesel',
        'Sewell Chan Nyt': 'Sewell Chan',
        'Shadi Rahimi Nyt': 'Shadi Rahimi',
        'Shaila Dewan Nyt': 'Shaila Dewan',
        'Sharon Lafraniere Nyt': 'Sharon LaFraniere',
        'Sharon Waxman Nyt': 'Sharon Waxman',
        'Sherri Day Nyt': 'Sherri Day',
        'Shimali Senanayake Nyt': 'Shimali Senanayake',
        'Simon Romero Nyt': 'Simon Romero',
        'Simon Shifrin Nyt': 'Simon Shifrin',
        'Somini Sengupta Nyt': 'Somini Sengupta',
        'Sonia Kishkovsky Nyt': 'Sonia Kishkovsky',
        'Sophia Chang Nyt': 'Sophia Chang',
        'Sophia Kishkovsky Nyt': 'Sophia Kishkovsky',
        'Stacey Stowe Nyt': 'Stacey Stowe',
        'Stacy Albin Nyt': 'Stacy Albin',
        'Stacy Stowe Nyt': 'Stacy Stowe',
        'Stefano Coledan Nyt': 'Stefano Coledan',
        'Stephanie Flanders Nyt': 'Stephanie Flanders',
        'Stephanie Saul Nyt': 'Stephanie Saul',
        'Stephanie Strom Nyt': 'Stephanie Strom',
        'Stephen Labaton Nyt': 'Stephen Labaton',
        'Steve Barnes Nyt': 'Steve Barnes',
        'Steve Friess Nyt': 'Steve Friess',
        'Steve Lohr Nyt': 'Steve Lohr',
        'Steve Strunksy Nyt': 'Steve Strunksy',
        'Steve Strunsky Nyt': 'Steve Strunsky',
        'Steven Erlanger Nyt': 'Steven Erlanger',
        'Steven Greenhouse Nyt': 'Steven Greenhouse',
        'Stuart Elliot': 'Stuart Elliott',        # typo variant of the advertising columnist
        'Stuart Elliot Nyt': 'Stuart Elliott',
        # Accent mark variants (API inconsistently strips/preserves diacritics)
        'Richard Perez-Pena': 'Richard Pérez-Peña',
        'Jere Longman': 'Jeré Longman',
        'Ceylan Yeğinsu': 'Ceylan Yeginsu',
        'Orlando Mayorquin': 'Orlando Mayorquín',
        'Stuart Elliott Nyt': 'Stuart Elliott',
        'Sual Hansell Nyt': 'Sual Hansell',
        'Suha Maayeh Nyt': 'Suha Maayeh',
        'Susan Catto Nyt': 'Susan Catto',
        'Susan Gotthelf Nyt': 'Susan Gotthelf',
        'Susan Sachs Nyt': 'Susan Sachs',
        'Susan Saulny Nyt': 'Susan Saulny',
        'Susan Stellin Nyt': 'Susan Stellin',
        'Suzanne Daley Nyt': 'Suzanne Daley',
        'Suzanne Kapner Nyt': 'Suzanne Kapner',
        'Tamar Lewin Nyt': 'Tamar Lewin',
        'Tara Bahrampour Nyt': 'Tara Bahrampour',
        'Terry Aguayo Nyt': 'Terry Aguayo',
        'Terry Aquayo Nyt': 'Terry Aquayo',
        'Terry Prisitn Nyt': 'Terry Prisitn',
        'Terry Pristin Nyt': 'Terry Pristin',
        'Thayer Evans Nyt': 'Thayer Evans',
        'Theo Emery Nyt': 'Theo Emery',
        'Thom Shanker Nyt': 'Thom Shanker',
        'Thomas Crampton Nyt': 'Thomas Crampton',
        'Thomas Fuller Nyt': 'Thomas Fuller',
        'Thomas Lueck Nyt': 'Thomas Lueck',
        'Tim Eaton Nyt': 'Tim Eaton',
        'Tim Golden Nyt': 'Tim Golden',
        'Tim Weiner Nyt': 'Tim Weiner',
        'Timothy Egan Nyt': 'Timothy Egan',
        'Timothy Pritchard Nyt': 'Timothy Pritchard',
        'Timothy Williams Nyt': 'Timothy Williams',
        'Tina Kelley Nyt': 'Tina Kelley',
        'Todd Benson Nyt': 'Todd Benson',
        'Todd Halvorson Nyt': 'Todd Halvorson',
        'Todd Zaun Nyt': 'Todd Zaun',
        'Tom Wright Nyt': 'Tom Wright',
        'Tom Zeller Nyt': 'Tom Zeller',
        'Toni Whitt Nyt': 'Toni Whitt',
        'Tony Smith Nyt': 'Tony Smith',
        'Tracie Rozhon Nyt': 'Tracie Rozhon',
        'Tracy Rozhon Nyt': 'Tracy Rozhon',
        'Tyler Kepner Nyt': 'Tyler Kepner',
        'Tyrone Richardson Nyt': 'Tyrone Richardson',
        'Vicki Vila Nyt': 'Vicki Vila',
        'Victor Homola Nyt': 'Victor Homola',
        'Victor Homolo Nyt': 'Victor Homolo',
        'Victoria Shannon Nyt': 'Victoria Shannon',
        'Viv Bernstein Nyt': 'Viv Bernstein',
        'Wade Rawlins Nyt': 'Wade Rawlins',
        'Walter Gibbs Nyt': 'Walter Gibbs',
        'Warren Hoge Nyt': 'Warren Hoge',
        'Warren Leary Nyt': 'Warren Leary',
        'Wayne Arnold Nyt': 'Wayne Arnold',
        'Wendy Ginsberg Nyt': 'Wendy Ginsberg',
        'William Beaver Nyt': 'William Beaver',
        'William Glaberson Nyt': 'William Glaberson',
        'William Neuman Nyt': 'William Neuman',
        'William Yardley Nyt': 'William Yardley',
        'Winnie Hu Nyt': 'Winnie Hu',
        'Yaniv Gafner Nyt': 'Yaniv Gafner',
        'Yilu Zhao Nyt': 'Yilu Zhao',
        'Zulfiqar Shah Nyt': 'Zulfiqar Shah',

        # Manual corrections: dropped middle names/initials not caught by auto-dedup
        # (both short and full forms had >10 articles, so the conservative threshold didn't fire)
        # Verified via beats, section, year range, and where available Wikipedia/LinkedIn.
        'Roni Rabin': 'Roni Caryn Rabin',
        'Jeremy Peters': 'Jeremy W. Peters',
        'Barnaby Feder': 'Barnaby J. Feder',
        'Claudia Deutsch': 'Claudia H. Deutsch',
        'Eric Taub': 'Eric A. Taub',
        'Mallery Lane': 'Mallery Roberts Lane',
        'Fred Bernstein': 'Fred A. Bernstein',
        'Chris Nicholson': 'Chris V. Nicholson',
        'Joyce Lau': 'Joyce Hor-chung Lau',
        'Jonah Bromwich': 'Jonah Engel Bromwich',
        'Adam Kepler': 'Adam W. Kepler',
        'Rachel Harris': 'Rachel Lee Harris',
        'Andrew Kramer': 'Andrew E. Kramer',       # Business/oil early career → Russia/World
        'Robert Worth': 'Robert F. Worth',         # Metro desk start → Middle East correspondent (Wikipedia)
        'Elizabeth Harris': 'Elizabeth A. Harris', # Metro → Business → Culture → Books (Wikipedia)
        'Pedro Rosado': 'Pedro Rafael Rosado',     # Same audio/video producer (MuckRack)
        'Natalia Osipova': 'Natalia V. Osipova',   # Same NYT video journalist (LinkedIn)
        # NOT merging: 'Robert Frank' / 'Robert H. Frank' — different people
        # (Robert H. Frank = Cornell economist/columnist; Robert Frank = wealth/lifestyle reporter)
        # Middle name present in most bylines but occasionally dropped (user-confirmed same person)
        'Michael Shear': 'Michael D. Shear',
    }

    # Apply overrides to all articles so counts accumulate on the correct name
    for art in articles:
        art["authors"] = [AUTHOR_OVERRIDES.get(a, a) for a in art["authors"]]

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
        "annual_blog_counts": defaultdict(int),  # year -> blog article count
        "annual_blog_words": defaultdict(int),   # year -> words from blog articles
        "shared_byline_count": 0,
        "monthly_shared_counts": defaultdict(int),  # YYYY-MM -> shared article count
        "coauthors": Counter(),
        "zero_word_articles": 0,
        "solo_text_articles": 0,  # solo bylines with word_count > 200
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
            if art["word_count"] == 0:
                d["zero_word_articles"] += 1
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

        # Normalize blog words using same scaling factors as annual_words_norm
        annual_blog_words_norm = {}
        for y in years:
            raw_total = d["annual_words"].get(y, 0)
            raw_blog = d["annual_blog_words"].get(y, 0)
            if raw_total > 0 and raw_blog > 0 and y in annual_words_norm:
                annual_blog_words_norm[y] = round(annual_words_norm[y] * raw_blog / raw_total)

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

        article_count = d["article_count"]
        shared_count = d["shared_byline_count"]
        zero_word_rate = d["zero_word_articles"] / article_count if article_count else 0
        shared_rate = shared_count / article_count if article_count else 0
        avg_words = round(d["total_words"] / article_count) if article_count else 0
        # Likely non-editorial / collaborative byline: photographers, video producers,
        # podcast staff, crossword constructors, illustrators, etc.
        # Four routes to flagging:
        #   1. Photo/video: high shared rate + many zero-word articles
        #   2. Low-word shared: nearly always shared + very low avg words (illustrators,
        #      photographers whose articles have captions but no bylined text)
        #   3. Podcast / audio: primary section is Podcasts
        #   4. Other structural: section is Crosswords & Games or Briefing + very high shared
        is_photo_video = (
            article_count >= 5 and
            shared_rate >= 0.75 and
            zero_word_rate >= 0.35
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
        likely_multimedia = (is_photo_video or is_low_word_shared or is_podcast or is_structural) and not has_reporting_history
        top_coauthors = dict(d["coauthors"].most_common(10))

        authors.append({
            "name": name,
            "article_count": article_count,
            "total_words": d["total_words"],
            "avg_words": round(d["total_words"] / article_count) if article_count else 0,
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
            "shared_byline_count": shared_count,
            "monthly_shared_counts": dict(d["monthly_shared_counts"]),
            "coauthors": top_coauthors,
            "likely_multimedia": likely_multimedia,
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
_GENERIC_PREFIXES = ('internal-', 'audio-', 'vis-', 'your-feed')
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


def is_blog_url(url):
    """Return True if the URL is a blog post (*.blogs.nytimes.com or dealbook.nytimes.com)."""
    if not url:
        return False
    try:
        domain = url.split('/')[2]
    except IndexError:
        return False
    return domain.endswith('.blogs.nytimes.com') or domain == 'dealbook.nytimes.com'


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


def build_dashboard_data(articles, authors):
    """Pre-compute dashboard statistics."""
    # Articles per month — with blog/non-blog split
    monthly = Counter()
    monthly_words = defaultdict(int)
    monthly_blog = Counter()
    monthly_nonblog = Counter()
    for art in articles:
        ym = art["year_month"]
        monthly[ym] += 1
        monthly_words[ym] += art["word_count"]
        if is_blog_url(art["web_url"]):
            monthly_blog[ym] += 1
        else:
            monthly_nonblog[ym] += 1

    months_sorted = sorted(monthly.keys())
    articles_per_month = [
        {
            "month": m,
            "count": monthly[m],
            "words": monthly_words[m],
            "blog": monthly_blog[m],
            "nonblog": monthly_nonblog[m],
        }
        for m in months_sorted
    ]

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
        return loc

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

    for art in articles:
        url = art.get("web_url", "") or ""
        ss = (art.get("subsection", "") or "").lower()
        sb = [s.lower() for s in (art.get("subjects", []) or [])]
        section = (art.get("section", "") or "").lower()
        is_vows_col = "vows (times column)" in sb
        # Strict wedding filter: subsection must be "Weddings", or URL in /fashion/weddings/
        # or /style/weddings/ path, or the explicit engagements subject in a style/fashion context.
        # Avoids false positives from generic "weddings" subject tag on trend/culture articles.
        is_weddings = (
            ss == "weddings"
            or "/fashion/weddings/" in url
            or "/style/weddings/" in url
            or is_vows_col
            or (
                "weddings and engagements" in sb
                and section in ("styles", "style", "fashion", "u.s.")
            )
        )
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
        ss = (art.get("subsection", "") or "").lower()
        sb = [s.lower() for s in (art.get("subjects", []) or [])]
        section = (art.get("section", "") or "").lower()
        is_vows_col = "vows (times column)" in sb
        is_wed = (
            ss == "weddings"
            or "/fashion/weddings/" in url
            or "/style/weddings/" in url
            or is_vows_col
            or ("weddings and engagements" in sb and section in ("styles", "style", "fashion", "u.s."))
        )
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
    recent_wedding_articles.sort(key=lambda x: x["d"], reverse=True)
    recent_wedding_articles = recent_wedding_articles[:400]

    # Merge author lists (announcements + Vows column together)
    all_wed_authors = Counter()
    for n, c in weddings_authors.items():
        all_wed_authors[n] += c
    for n, c in vows_col_authors.items():
        all_wed_authors[n] += c

    features_data = {
        "weddings": {
            "by_year": dict(weddings_by_year),
            "vows_col_by_year": dict(vows_col_by_year),
            "top_authors": [{"name": n, "count": c} for n, c in all_wed_authors.most_common(15)],
            "recent_articles": recent_wedding_articles,
            "total": sum(weddings_by_year.values()),
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
        "sections": sections,
        "section_trends": section_trends,
        "all_years": all_years,
        "top_authors": top_authors,
        "wordiest_authors": wordiest,
        "multi_byline_trend": multi_byline_trend,
        "world_coverage": world_coverage,
        "us_state_coverage": us_state_coverage,
        "features": features_data,
    }


def _normalize_subject_name(name):
    """Title-case ALL CAPS names; leave acronyms (no spaces/commas) alone."""
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

    def make_entries(annual_dict):
        result = []
        for name, by_year in annual_dict.items():
            total = sum(by_year.values())
            if total < MIN_COUNT:
                continue
            result.append({"name": name, "total": total, "annual": dict(by_year)})
        result.sort(key=lambda x: x["total"], reverse=True)
        return result

    return {
        "persons": make_entries(persons_annual),
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
            rec["g"] = a["glocations"]  # glocations
        if a.get("canonical_states"):
            rec["st"] = a["canonical_states"]  # canonical US state names
        if a.get("subsection"):
            rec["ss"] = a["subsection"]  # subsection
        if a.get("subjects"):
            rec["sb"] = a["subjects"]  # subject keywords
        if a.get("persons"):
            rec["pe"] = a["persons"]   # persons keywords
        if a.get("organizations"):
            rec["og"] = a["organizations"]  # organizations keywords
        if a.get("print_headline"):
            rec["ph"] = a["print_headline"]  # print headline (omit if empty to save space)
        by_year[a["year"]].append(rec)

    years = sorted(by_year.keys())
    for year in years:
        filepath = os.path.join(DATA_DIR, f"articles_{year}.json")
        with open(filepath, "w") as f:
            json.dump(by_year[year], f, separators=(',', ':'))
    print(f"Saved article files for {len(years)} years ({sum(len(v) for v in by_year.values()):,} articles)")

    # Only export authors with >= 2 articles; exclude institutional bylines
    authors_export = [
        a for a in authors
        if a["article_count"] >= 2 and a["name"] not in _INSTITUTIONAL_BYLINES
    ]
    with open(os.path.join(DATA_DIR, "authors.json"), "w") as f:
        json.dump(authors_export, f)
    print(f"Saved authors.json ({len(authors_export):,} authors, >= 2 articles)")

    with open(os.path.join(DATA_DIR, "dashboard.json"), "w") as f:
        json.dump(dashboard, f)
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
