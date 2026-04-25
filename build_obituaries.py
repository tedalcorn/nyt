"""Extract obituary records from raw Archive API dumps and write data/obituaries.json."""
import json, os, re, glob, sys
from collections import Counter

RAW_DIR = 'data/raw'
OUT_PATH = 'data/obituaries.json'

# Name on left of first comma (also tolerates en-dash separator).
RE_NAME_COMMA = re.compile(r'^([^,]+?),\s*[A-Za-z\d\u2018\u2019\u201C\u201D\u00C0-\u017F\'"]')
RE_NAME_DASH = re.compile(r'^([A-Z][\w.\'\-\s]+?)\s*[\u2014\u2013-]\s*[A-Z]')
# Headline contains a death verb anywhere — pull the leading 1-4 capitalized
# tokens as the name. Handles:
#   - "Laurence Mancuso Dies; Founding Abbot Was 72" (semicolon, no age-after)
#   - "Whitey Bulger Is Dead in Prison at 89" (intervening "in Prison")
#   - "Joe Moakley of Massachusetts Dies at 74" (intervening "of …" clause)
#   - "Derek Freeman Dies at 84"
# Token whitelist excludes verb-form words that are also capitalized in
# headlines (Is, Was, Dies, Dead, Has, Had).
RE_HAS_DIES = re.compile(r'\b(?:Dies?|Is\s+Dead|Is\s+Dying|Was\s+Dead)\b', re.I)
_NOT_VERB = r'(?!(?:Is|Was|Has|Had|Will|Are|Were|Dies?|Dead|Died|Dying|From|The)\b)'
RE_LEADING_CAPS = re.compile(
    r'^(' + _NOT_VERB + r'[A-Z][\w.\'\-\u00C0-\u017F]*'
    r'(?:\s+' + _NOT_VERB + r'[A-Z][\w.\'\-\u00C0-\u017F]*){0,3})'
)

RE_AGE_DIES = re.compile(r'\b(?:Die[ds]?|Is\s+Dead|Dead)\s+at\s+(\d{2,3})\b', re.I)
RE_AGE_COMMA_HEAD = re.compile(r',\s*(\d{2,3})\s*[,\b]')
RE_AGE_WAS = re.compile(r'\bwas\s+(\d{2,3})\b', re.I)

# Common first names for gender fallback. Top-50 each, US 1940-2010 SSA.
MALE_NAMES = set("""
James John Robert Michael William David Richard Joseph Thomas Charles
Christopher Daniel Matthew Anthony Donald Mark Paul Steven Andrew Kenneth
George Joshua Kevin Brian Edward Ronald Timothy Jason Jeffrey Ryan Gary
Nicholas Eric Stephen Jonathan Larry Justin Scott Frank Brandon Benjamin
Gregory Samuel Raymond Patrick Alexander Jack Dennis Jerry Tyler Aaron
Henry Douglas Peter Jose Adam Nathan Zachary Walter Kyle Harold Carl Jeremy
Keith Roger Gerald Ethan Arthur Terry Christian Sean Lawrence Austin Joe
Noah Jesse Albert Bryan Billy Bruce Willie Jordan Dylan Alan Ralph Gabriel
Roy Juan Wayne Eugene Logan Randy Louis Russell Vincent Philip Bobby Johnny
Bradley Curtis Kenneth Howard Fred Stuart Lewis Theodore Ricky Earl Phillip
Phil Marvin Dale Cecil Floyd Clyde Norman Allen Ernest Stanley Leonard
Lloyd Bernard Glenn Eddie Calvin Travis Marcus Jeremiah Don Lance Cody
Mickey Maurice Wesley Stan Sidney Sam Dean Glen Marshall Dick Pat Toby
Edwin Tom Dwight Otis Bert Aldo Jorge Manuel Pedro Federico Carlos Luis
Jaime Jesus Roberto Mario Alfonso Anibal Alejandro Lazaro
""".split())

FEMALE_NAMES = set("""
Mary Patricia Linda Barbara Elizabeth Jennifer Maria Susan Margaret Dorothy
Lisa Nancy Karen Betty Helen Sandra Donna Carol Ruth Sharon Michelle Laura
Sarah Kimberly Deborah Jessica Shirley Cynthia Angela Melissa Brenda Amy
Anna Rebecca Virginia Kathleen Pamela Martha Debra Amanda Stephanie Carolyn
Christine Marie Janet Catherine Frances Ann Joyce Diane Alice Julie Heather
Teresa Doris Gloria Evelyn Jean Cheryl Mildred Katherine Joan Ashley Judith
Rose Janice Kelly Nicole Judy Christina Kathy Theresa Beverly Denise Tammy
Irene Jane Lori Rachel Marilyn Andrea Kathryn Louise Sara Anne Jacqueline
Wanda Bonnie Julia Ruby Lois Tina Phyllis Norma Paula Diana Annie Lillian
Emily Robin Peggy Crystal Gladys Rita Dawn Connie Florence Tracy Edna
Tiffany Carmen Rosa Cindy Grace Wendy Victoria Edith Kim Sherry Sylvia
Josephine Thelma Shannon Sheila Ethel Ellen Elaine Marjorie Carrie Charlotte
Monica Esther Pauline Emma Juanita Anita Rhonda Hazel Amber Eva Debbie April
Leslie Clara Lucille Jamie Joanne Eleanor Valerie Danielle Megan Alicia Suzanne
Michele Gail Bertha Darlene Veronica Jill Erin Geraldine Lauren Cathy Joann
Lorraine Lynn Sally Regina Erica Beatrice Dolores Bernice Audrey Yvonne
Annette June Marion Dana Stacy Ana Renee Ida Vivian Roberta Holly Brittany
Melanie Loretta Yolanda Jeanette Laurie Katie Kristen Vanessa Alma Sue Elsie
Beth Jeanne Vicki Carla Tara Rosemary Eileen Terri Gertrude Lucy Tonya Ella
Stacey Wilma Gina Kristin Jessie Natalie Agnes Vera Willie Charlene Bessie
Delores Melinda Pearl Arlene Maureen Colleen Allison Tamara Joy Georgia
Constance Lillie Claudia Jackie Marcia Tanya Nellie Minnie Marlene Heidi
Glenda Lydia Viola Courtney Marian Stella Caroline Dora Jo Vickie Mattie
Nina Ophelia Pippa
""".split())


def clean_smart_quotes(s):
    """Drop curly/straight quotes that confuse the parser."""
    if not s: return s
    return re.sub(r'[\u2018\u2019\u201C\u201D\'"]+', '', s).strip()


# Honorifics / titles to peel off the start of the name.
# Long-form first (so "The Reverend" matches before "Rev"); each entry includes
# trailing dot variants. Strips these before the name itself.
TITLE_PREFIXES = (
    r'(?:The\s+)?(?:Reverend|Rev\.?|Rev|Father|Fr\.?|Pastor|Bishop|Cardinal|'
    r'Sister|Brother|Mother|Rabbi|Imam|Sheikh|Sheik|Sri|Mahatma|'
    r'Sir|Dame|Lord|Lady|Baron|Baroness|Count|Countess|Duke|Duchess|'
    r'Prince|Princess|King|Queen|Emperor|Empress|'
    r'Dr\.?|Doctor|Professor|Prof\.?|Justice|Judge|Senator|Sen\.?|'
    r'Representative|Rep\.?|Governor|Gov\.?|Mayor|President|'
    r'General|Gen\.?|Colonel|Col\.?|Major|Maj\.?|Captain|Capt\.?|'
    r'Lieutenant|Lt\.?|Admiral|Adm\.?|Commander|Cmdr\.?|Sergeant|Sgt\.?|'
    r'Mr\.?|Mrs\.?|Ms\.?|Mx\.?|Miss|'
    r'Madame|Madam|Monsieur|Mademoiselle|'
    r'Se(?:n|ñ)or|Se(?:n|ñ)ora|Se(?:n|ñ)orita|'
    r'Sultan|Sultana|Emir|Sheikh|Sheik|'
    r'Saint|St\.?)'
)
RE_LEADING_TITLE = re.compile(r'^' + TITLE_PREFIXES + r'\s+', re.I)
# "What They Left Behind:" is a recurring NYT Magazine end-of-year series
RE_LEADING_SERIES = re.compile(
    r'^(?:Overlooked No More|What They Left Behind|The Lives They Lived|Lives They Lived|'
    r'A Life Lived|Living On|In Memoriam)\s*[:\u2014\u2013-]\s*',
    re.I,
)
# "From 1992: …" — Times republishes old obits as packages (e.g., Women's
# History Month). Distinct from Overlooked No More (which is *new* coverage of
# someone never covered). Strip the prefix so the original headline can parse.
# Also tolerates a stacked "From From 1992:" / "From: From 1992:" the desk
# sometimes lets through.
RE_FROM_YEAR = re.compile(
    r'^(?:From(?:\s*[:\u2014\u2013-]\s*|\s+))?From\s+\d{4}\s*[:\u2014\u2013-]\s*|'
    r'^From\s+\d{4}\s*[:\u2014\u2013-]\s*',
    re.I,
)
# Republished-obit boilerplate that prepends the lead paragraph. We strip it
# before pronoun/honorific scanning so gender detection sees the actual prose.
RE_REPUB_BOILER = re.compile(
    r'This\s+obituary\s+was\s+originally\s+published\s+on[^.]+\.[^.]*?'
    r'(?:republished|reissued)[^.]*?\.\s*',
    re.I,
)
# Year-end / memoriam roundup slugs that aren't single-subject obituaries.
# Slideshows and videos are also dropped — many sit on the Obits desk but
# carry no parseable name in the headline (e.g. "A Look at Hiro's Work").
RE_NON_OBIT_URL = re.compile(
    r'(?:'
    r'obituaries-deaths-\d{4}|'         # year-end roundups: /obituaries-deaths-2023
    r'/learning/lesson-plans/|'          # NYT Learning lesson plans
    r'/slideshow/|'                      # photo packages
    r'/video/|'                          # video obits
    r'in-a-political-year-some-deaths|'  # 2024 Navalny package
    r'lives-they-lived'                  # NYT Magazine year-end issue
    r')',
    re.I,
)
# 9/11 "Portraits of Grief" — published Dec 2001 - Sep 2002, ~1,800 articles.
# Profile-style obits: headline is "Name: Tagline", desk='National / Portraits
# of Grief', URL contains '/national/portraits/'. Standard parsers fail
# because they have no death verb in the headline.
RE_PORTRAITS_URL = re.compile(r'/national/portraits/', re.I)
RE_PORTRAITS_DESK = re.compile(r'Portraits of Grief', re.I)
RE_PORTRAITS_HEADLINE = re.compile(r'^([A-Z][\w.\'\-\u00C0-\u017F]+(?:\s+[A-Z][\w.\'\-\u00C0-\u017F]+){0,4}):\s*\S')
# Headlines that mark group / multi-subject / package pieces, not single obits
RE_GROUP_HEADLINE = re.compile(
    r'^(?:Lesson of the Day|The Lives They Lived|Year in Review|'
    r'In a Political Year|Obituaries: Deaths in)\b',
    re.I,
)


def extract_name_from_slug(url):
    """Extract a candidate name from URL slug as last-resort fallback.
    /2021/06/08/sports/football/jim-fassel-giants-dead.html → "Jim Fassel"
    /2022/08/12/us/anne-heche-brain-injury.html → "Anne Heche"
    /2007/07/01/nyregion/01mancuso.html → None (single token, no first name)
    Returns None if the slug yields fewer than 2 plausible name tokens.
    """
    if not url: return None
    # Get terminal slug (path component before .html), drop leading date digits
    m = re.search(r'/([a-z0-9][a-z0-9\-]+?)(?:\.html?)?$', url, re.I)
    if not m: return None
    slug = m.group(1).lower()
    slug = re.sub(r'^\d+[-_]?', '', slug)               # drop leading "01" etc.
    slug = re.sub(r'(?:[-_](?:dead|dies|obituary|obit))+$', '', slug)
    parts = [p for p in re.split(r'[-_]+', slug)
             if p and not p.isdigit() and len(p) > 1
             and p not in {'cnd', 'and', 'the', 'at', 'on', 'in', 'of', 'web', 'index'}]
    if len(parts) < 2: return None
    # Take first 2-3 tokens as the name guess. Stop if we hit a token that
    # looks like noise ("giants", "uruguay", "brain") — i.e., if a token
    # doesn't look like a typical surname and we already have 2 tokens, stop.
    name_parts = parts[:2]
    if len(parts) >= 3 and len(parts[2]) >= 4 and parts[2][0].isalpha():
        # Add third token only if it could plausibly be part of a name
        # (e.g. middle names, titles). Heuristic: short third token unlikely.
        # Conservative — don't add for now to avoid "Jim Fassel Giants".
        pass
    return ' '.join(p.capitalize() for p in name_parts)


def extract_name(headline, url=None, is_portraits=False):
    if not headline: return None
    # Drop zero-width chars
    h = headline.replace('\u200b', '').replace('\ufeff', '').strip()
    # Portraits of Grief: headline is "Name: Tagline" — colon-separated.
    if is_portraits:
        m = RE_PORTRAITS_HEADLINE.match(h)
        if m:
            cand = m.group(1).strip()
            if 1 <= cand.count(' ') <= 4:
                return cand
    # Strip "From YYYY:" republished-obit prefix (apply before series check
    # since some headlines stack "From 1992: Marlene Dietrich Is Dead")
    h = RE_FROM_YEAR.sub('', h)
    # Strip recurring series prefixes (Overlooked No More, etc.)
    h = RE_LEADING_SERIES.sub('', h)
    # Strip parentheticals — "Barry Humphries (Dame Edna to You, Possums) Is
    # Dead at 89" should parse as "Barry Humphries Is Dead at 89".
    h = re.sub(r'\s*\([^)]*\)', '', h)
    # Strip leading "'Nickname': " prefixes
    h = re.sub(r'^[\u2018\u201C\'"][^\u2019\u201D\'"]+[\u2019\u201D\'"]\s*[:,]\s*', '', h)
    # Strip honorific titles (The Reverend, Sir, Dr., Representative, etc.)
    # Apply twice in case there are stacked titles ("Rep. Dr. ...").
    h = RE_LEADING_TITLE.sub('', h)
    h = RE_LEADING_TITLE.sub('', h)
    m = RE_NAME_COMMA.match(h)
    if m:
        cand = m.group(1).strip()
        # Reject if "Is Dead" / "Dies at" appears inside the candidate — that
        # means the comma fell after the verb phrase, not after the name.
        # ("D. Avramovic Is Dead at 81; Reform…" wraps name in candidate text)
        if RE_HAS_DIES.search(cand):
            cand = RE_HAS_DIES.split(cand)[0].strip().rstrip(',;:.')
            cand = re.sub(r'\s+(?:Is|Has|Was)$', '', cand)
        # Reject obvious non-names
        if any(w in cand.lower() for w in (' was ', ' is ', ' has ', ' will ')): return None
        # Allow single-word names (Birendra, Cher, Madonna) up to 6-token compound names
        if cand and 0 <= cand.count(' ') <= 6:
            return cand
    # Headlines with "Dies at N" but no comma (or comma falls after verb):
    # capture leading 1-4 capitalized tokens. Handles "Joe Moakley of
    # Massachusetts Dies at 74", "Derek Freeman Dies at 84".
    if RE_HAS_DIES.search(h):
        m = RE_LEADING_CAPS.match(h)
        if m:
            return m.group(1).strip()
    m = RE_NAME_DASH.match(h)
    if m:
        return m.group(1).strip()
    return None


def extract_age(headline, abstract):
    for txt in (headline, abstract):
        if not txt: continue
        m = RE_AGE_DIES.search(txt)
        if m: return int(m.group(1))
    if headline:
        m = RE_AGE_COMMA_HEAD.search(headline)
        if m and 18 <= int(m.group(1)) <= 120: return int(m.group(1))
    if abstract:
        m = RE_AGE_WAS.search(abstract)
        if m and 18 <= int(m.group(1)) <= 120: return int(m.group(1))
    return None


def extract_gender(name, full_text):
    """Return (gender, source) where source is one of:
        'honorific' — Mr./Mrs./Sir/Dame/Lord/etc. in text (highest confidence)
        'pronoun'   — he/she/his/her tally in text (high confidence)
        'first_name' — match against US baby-name lists (lowest confidence;
                       culture-specific and unreliable for non-Western names)
        None         — could not determine
    """
    if full_text:
        t = ' ' + full_text + ' '  # case-sensitive — honorifics are capitalized
        # Strong honorific: title immediately followed by a capitalized name token.
        # More reliable than bare "Dame" (which appears in stage names like Dame
        # Edna) or bare "Sir/Lord" (titles for someone other than the subject).
        m_strong = len(re.findall(r'\bMr\.?\s+[A-Z]', t))
        f_strong = (len(re.findall(r'\bMrs\.?\s+[A-Z]', t))
                    + len(re.findall(r'\bMs\.?\s+[A-Z]', t)))
        if m_strong and m_strong > f_strong: return ('M', 'honorific')
        if f_strong and f_strong > m_strong: return ('F', 'honorific')
        m_hon = (m_strong + len(re.findall(r'\bSir\s', t))
                 + len(re.findall(r'\b(?:Lord|Baron|Count|Duke|Prince|King|Emperor)\s', t)))
        f_hon = (f_strong + len(re.findall(r'\b(?:Dame|Lady|Baroness|Countess|Duchess|Princess|Queen|Empress|Madame|Madam)\s', t)))
        if m_hon and not f_hon: return ('M', 'honorific')
        if f_hon and not m_hon: return ('F', 'honorific')
        tl = t.lower()
        he = tl.count(' he ') + tl.count(' his ') + tl.count(' him ')
        she = tl.count(' she ') + tl.count(' her ') + tl.count(' herself ')
        # Single-direction signal accepted at 1+ (catches short abstracts)
        if he >= 1 and she == 0: return ('M', 'pronoun')
        if she >= 1 and he == 0: return ('F', 'pronoun')
        # Both directions: require margin
        if he >= 2 and he > she * 1.5: return ('M', 'pronoun')
        if she >= 2 and she > he * 1.5: return ('F', 'pronoun')
    # First-name fallback (US baby-name lists; flagged as low-confidence)
    if name:
        first = name.split()[0].rstrip(',.').strip()
        first = re.sub(r'[\u2018\u2019\u201C\u201D\'"]', '', first)
        if first in MALE_NAMES: return ('M', 'first_name')
        if first in FEMALE_NAMES: return ('F', 'first_name')
    return (None, None)


def extract_profession(headline):
    if not headline: return None
    h = re.sub(r'^Overlooked No More:\s*', '', headline, flags=re.I)
    h = re.sub(r'^[\u2018\u201C\'"][^\u2019\u201D\'"]+[\u2019\u201D\'"]\s*[:,]\s*', '', h)
    parts = h.split(',')
    if len(parts) < 2: return None
    role = parts[1].strip()
    # Drop subordinate clauses
    role = re.sub(r'\s+(?:who|that|whose|which)\b.*$', '', role, flags=re.I)
    role = re.sub(r'\b(?:is\s+|was\s+)?(?:dies|dead|died|is\s+dead)\b.*$', '', role, flags=re.I).strip()
    # Strip leading "Who/That/Of " when role begins with relative-clause continuation
    role = re.sub(r'^(?:who|that|whose|of|with|by)\s+', '', role, flags=re.I).strip()
    role = clean_smart_quotes(role)
    role = role.rstrip('.,;:').strip()
    if not role or role.isdigit(): return None
    if len(role) < 3 or len(role) > 80: return None
    return role


def main():
    files = sorted(glob.glob(os.path.join(RAW_DIR, '*.json')))
    print(f"Scanning {len(files)} monthly raw files...")

    all_obits = []
    by_year = Counter()
    skipped_corr = 0
    skipped_non_obit = 0

    # Death-marker patterns that confirm a single-subject obit even when the
    # article was caught only via news_desk=Obits / section=Obituaries.
    RE_DEATH_HEADLINE = re.compile(
        r'\b(?:Dies?\s+at\s+\d|Is\s+Dead\s+at\s+\d|Dead\s+at\s+\d|Dies?|Is\s+Dead|'
        r'Dead\b|,\s*\d{2,3}\s*,)',
        re.I,
    )
    RE_OBIT_URL_HINT = re.compile(r'-(?:dead|dies|obituary)\b|/obituaries/', re.I)

    for f in files:
        with open(f) as fh:
            try:
                docs = json.load(fh)
            except Exception:
                continue
        for d in docs:
            tom = d.get('type_of_material', '') or ''
            news_desk = d.get('news_desk', '') or ''
            section = d.get('section_name', '') or ''
            url = d.get('web_url', '')
            if url.startswith('https://www.nytimes.com'):
                url = url.replace('https://www.nytimes.com', '')
            # Skip corrections — they have a dedicated Corrections tab and
            # aren't actual obituaries. Common in 2008-2010 ("For The Record"
            # daily obit corrections column).
            if tom == 'Correction':
                skipped_corr += 1
                continue
            # Identify obits — accept either type tag or Obits desk OR Obituaries section.
            # Also include the 9/11 "Portraits of Grief" series whose desk is
            # "National / Portraits of Grief" and whose headlines have no death verb.
            is_portraits = bool(
                RE_PORTRAITS_DESK.search(news_desk)
                or RE_PORTRAITS_URL.search(url)
            )
            is_obit = (tom == 'Obituary (Obit)'
                       or tom == 'Obituary'
                       or news_desk == 'Obits'
                       or section == 'Obituaries'
                       or is_portraits)
            if not is_obit:
                continue
            # Drop slideshows and videos — many sit on the Obits desk but carry
            # no parseable subject in the headline.
            if tom in ('Slideshow', 'Video', 'Audio'):
                skipped_non_obit += 1
                continue

            h = d.get('headline', {}).get('main', '') or ''

            # Hard rejects: year-end roundups, lesson plans, multi-subject packages,
            # slideshows/videos by URL pattern
            if RE_NON_OBIT_URL.search(url) or RE_GROUP_HEADLINE.match(h):
                skipped_non_obit += 1
                continue
            # Even when tom='Obituary (Obit)' the article may be an essay-style
            # appreciation that lacks a name in the headline (Pepe Mujica,
            # Jimmy Cliff). Require a death-marker, obit-style URL slug, or a
            # known series prefix — otherwise drop. This catches both:
            #   - desk/section-only obits that aren't really obits (related
            #     articles riding the Obits desk byline)
            #   - tom-tagged appreciation pieces with no name in the headline
            # Portraits of Grief bypass this gate — they're profile-style.
            looks_like_obit = bool(
                is_portraits
                or RE_DEATH_HEADLINE.search(h)
                or RE_OBIT_URL_HINT.search(url)
                or RE_LEADING_SERIES.match(h)
                or RE_FROM_YEAR.match(h)
            )
            if not looks_like_obit:
                skipped_non_obit += 1
                continue

            ab = d.get('abstract', '') or ''
            snip = d.get('snippet', '') or ''
            lead = d.get('lead_paragraph', '') or ''
            # For republished obits, strip the boilerplate "This obituary was
            # originally published…" sentence so gender/age detection sees the
            # actual obit prose underneath.
            republished = bool(RE_FROM_YEAR.match(h)) or bool(
                re.search(r'(?:originally\s+published|being\s+republished)', lead, re.I)
            )
            lead_clean = RE_REPUB_BOILER.sub('', lead) if republished else lead
            full = ' '.join([ab, snip, lead_clean])

            name = extract_name(h, url, is_portraits=is_portraits)
            # Slug-based fallback for "essay-style" obit headlines that don't
            # parse — Anne Heche, Jim Fassel, etc. Only when the URL slug
            # itself reads like a name.
            if not name:
                name = extract_name_from_slug(url)
            age = extract_age(h, ab + ' ' + snip + ' ' + lead_clean)
            prof = extract_profession(h)
            gen, gen_src = extract_gender(name, full)
            overlooked = bool(re.match(r'^Overlooked No More\b', h, re.I))

            pub = d.get('pub_date', '')[:10]
            year = pub[:4] if pub else ''

            all_obits.append({
                'name': name,
                'age': age,
                'gender': gen,
                'gender_src': gen_src,
                'profession': prof,
                'overlooked': overlooked,
                'republished': republished,
                'portraits': is_portraits,
                'date': pub,
                'year': year,
                'section': section,
                'desk': news_desk,
                'tom': tom,
                'headline': h,
                'abstract': ab,
                'url': url,
            })
            by_year[year] += 1

    # Merge same-name records published within ±10 days. The Times often runs
    # an initial obit and a follow-up profile within a week (Peter Gowland:
    # 2010-04-01 and 2010-04-05). Keep the canonical entry as primary, but
    # surface the secondary URL so the reader can see it.
    from datetime import date as _date
    _PREF = {'Obituary (Obit)': 0, 'Obituary': 1, 'Obituary; Biography': 2}
    def _rank(o):
        # Lower = preferred. tom rank, then length of headline (longer = more
        # specific), then negated url length so the longer slug wins on ties.
        return (_PREF.get(o.get('tom') or '', 9), -len(o.get('headline') or ''),
                -len(o.get('url') or ''))
    def _parse_date(s):
        try: return _date(int(s[:4]), int(s[5:7]), int(s[8:10]))
        except Exception: return None

    # Group by name; within each group, cluster by date proximity.
    by_name = {}
    no_name_rows = []
    for o in all_obits:
        if not o.get('name'):
            no_name_rows.append(o)
            continue
        by_name.setdefault(o['name'], []).append(o)

    merged = []
    n_merged = 0
    for name, recs in by_name.items():
        recs_sorted = sorted(recs, key=lambda o: o.get('date') or '')
        clusters = []
        for r in recs_sorted:
            d = _parse_date(r.get('date') or '')
            if not clusters:
                clusters.append([r])
                continue
            last = clusters[-1][-1]
            d2 = _parse_date(last.get('date') or '')
            if d and d2 and abs((d - d2).days) <= 10:
                clusters[-1].append(r)
            else:
                clusters.append([r])
        for cluster in clusters:
            if len(cluster) == 1:
                merged.append(cluster[0])
            else:
                # Pick canonical primary, attach secondary URLs from the rest
                primary = sorted(cluster, key=_rank)[0]
                others = [c for c in cluster if c is not primary]
                primary['secondary_urls'] = [c.get('url') for c in others if c.get('url')]
                primary['secondary_dates'] = [c.get('date') for c in others if c.get('date')]
                merged.append(primary)
                n_merged += len(others)
    merged.extend(no_name_rows)
    n_total_before = len(all_obits)
    print(f"Skipped {skipped_corr:,} correction-notice records (paper-wide tom='Correction', "
          f"used by Corrections tab; not corrections to obits), {skipped_non_obit:,} non-obit "
          f"package/lesson articles; merged {n_merged:,} same-name near-duplicates "
          f"(±10 days). {n_total_before:,} → {len(merged):,}")
    all_obits = merged
    n_repub = sum(1 for o in all_obits if o.get('republished'))
    n_overl = sum(1 for o in all_obits if o.get('overlooked'))
    n_port = sum(1 for o in all_obits if o.get('portraits'))
    print(f"  republished: {n_repub:,}   overlooked-no-more: {n_overl:,}   "
          f"portraits-of-grief: {n_port:,}")

    print(f"\nTotal obits: {len(all_obits):,}")
    print("By year:")
    for y in sorted(by_year):
        print(f"  {y}: {by_year[y]:,}")

    # Coverage stats
    n = len(all_obits)
    n_name = sum(1 for o in all_obits if o['name'])
    n_age = sum(1 for o in all_obits if o['age'])
    n_gen = sum(1 for o in all_obits if o['gender'])
    n_prof = sum(1 for o in all_obits if o['profession'])
    print(f"\nCoverage of n={n:,}:")
    print(f"  name:       {n_name:,} ({100*n_name/n:.0f}%)")
    print(f"  age:        {n_age:,} ({100*n_age/n:.0f}%)")
    print(f"  gender:     {n_gen:,} ({100*n_gen/n:.0f}%)")
    print(f"  profession: {n_prof:,} ({100*n_prof/n:.0f}%)")
    print(f"\n  gender breakdown: M={sum(1 for o in all_obits if o['gender']=='M'):,} "
          f"F={sum(1 for o in all_obits if o['gender']=='F'):,}")
    src_count = Counter(o.get('gender_src') for o in all_obits if o.get('gender'))
    print(f"  gender by source:")
    for s in ('honorific', 'pronoun', 'first_name'):
        n = src_count.get(s, 0)
        pct = 100 * n / max(1, sum(src_count.values()))
        print(f"    {s:12s} {n:>6,}  ({pct:.1f}%)")

    with open(OUT_PATH, 'w') as f:
        json.dump(all_obits, f, separators=(',', ':'))
    print(f"\nSaved {OUT_PATH} ({os.path.getsize(OUT_PATH):,} bytes)")


if __name__ == '__main__':
    main()
