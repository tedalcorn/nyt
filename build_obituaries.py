"""Extract obituary records from raw Archive API dumps and write data/obituaries.json."""
import json, os, re, glob, sys
from collections import Counter

RAW_DIR = 'data/raw'
OUT_PATH = 'data/obituaries.json'

# Name on left of first comma (also tolerates en-dash separator).
RE_NAME_COMMA = re.compile(r'^([^,]+?),\s*[A-Za-z\d\u2018\u2019\u201C\u201D\u00C0-\u017F\'"]')
RE_NAME_DASH = re.compile(r'^([A-Z][\w.\'\-\s]+?)\s*[\u2014\u2013-]\s*[A-Z]')

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
    r'Saint|St\.?)'
)
RE_LEADING_TITLE = re.compile(r'^' + TITLE_PREFIXES + r'\s+', re.I)
# "What They Left Behind:" is a recurring NYT Magazine end-of-year series
RE_LEADING_SERIES = re.compile(
    r'^(?:Overlooked No More|What They Left Behind|The Lives They Lived|Lives They Lived|'
    r'A Life Lived|Living On|In Memoriam)\s*[:\u2014\u2013-]\s*',
    re.I,
)


def extract_name(headline):
    if not headline: return None
    # Drop zero-width chars
    h = headline.replace('\u200b', '').replace('\ufeff', '').strip()
    # Strip recurring series prefixes (Overlooked No More, etc.)
    h = RE_LEADING_SERIES.sub('', h)
    # Strip leading "'Nickname': " prefixes
    h = re.sub(r'^[\u2018\u201C\'"][^\u2019\u201D\'"]+[\u2019\u201D\'"]\s*[:,]\s*', '', h)
    # Strip honorific titles (The Reverend, Sir, Dr., etc.)
    h = RE_LEADING_TITLE.sub('', h)
    m = RE_NAME_COMMA.match(h)
    if m:
        cand = m.group(1).strip()
        # Reject obvious non-names
        if any(w in cand.lower() for w in (' was ', ' is ', ' has ', ' will ')): return None
        if cand and 1 <= cand.count(' ') <= 6:
            return cand
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
    # Honorific-based (very high signal): Mr./Mrs./Ms./Sir/Lord/etc.
    if full_text:
        t = ' ' + full_text + ' '  # case-sensitive — these are typically capitalized
        # Count male/female honorifics
        m_hon = (len(re.findall(r'\bMr\.?\s', t)) + len(re.findall(r'\bSir\s', t))
                 + len(re.findall(r'\b(?:Lord|Baron|Count|Duke|Prince|King|Emperor)\s', t)))
        f_hon = (len(re.findall(r'\bMrs\.?\s', t)) + len(re.findall(r'\bMs\.?\s', t))
                 + len(re.findall(r'\b(?:Dame|Lady|Baroness|Countess|Duchess|Princess|Queen|Empress|Madame|Madam)\s', t)))
        if m_hon and not f_hon: return 'M'
        if f_hon and not m_hon: return 'F'
        # Pronoun tally
        tl = t.lower()
        he = tl.count(' he ') + tl.count(' his ') + tl.count(' him ')
        she = tl.count(' she ') + tl.count(' her ') + tl.count(' herself ')
        # If only one direction has signal, accept on >=1 (catches short abstracts)
        if he >= 1 and she == 0: return 'M'
        if she >= 1 and he == 0: return 'F'
        # Both directions: require margin
        if he >= 2 and he > she * 1.5: return 'M'
        if she >= 2 and she > he * 1.5: return 'F'
    # First-name fallback
    if name:
        first = name.split()[0].rstrip(',.').strip()
        # Strip nickname quotes
        first = re.sub(r'[\u2018\u2019\u201C\u201D\'"]', '', first)
        if first in MALE_NAMES: return 'M'
        if first in FEMALE_NAMES: return 'F'
    return None


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
            # Skip corrections — they have a dedicated Corrections tab and
            # aren't actual obituaries. Common in 2008-2010 ("For The Record"
            # daily obit corrections column).
            if tom == 'Correction':
                skipped_corr += 1
                continue
            # Identify obits — accept either type tag or Obits desk OR Obituaries section
            is_obit = (tom == 'Obituary (Obit)'
                       or tom == 'Obituary'
                       or news_desk == 'Obits'
                       or section == 'Obituaries')
            if not is_obit:
                continue
            h = d.get('headline', {}).get('main', '') or ''
            ab = d.get('abstract', '') or ''
            snip = d.get('snippet', '') or ''
            lead = d.get('lead_paragraph', '') or ''
            full = ' '.join([ab, snip, lead])

            name = extract_name(h)
            age = extract_age(h, ab + ' ' + snip + ' ' + lead)
            prof = extract_profession(h)
            gen = extract_gender(name, full)
            overlooked = bool(re.match(r'^Overlooked No More\b', h, re.I))

            pub = d.get('pub_date', '')[:10]
            year = pub[:4] if pub else ''
            url = d.get('web_url', '')
            if url.startswith('https://www.nytimes.com'):
                url = url.replace('https://www.nytimes.com', '')

            all_obits.append({
                'name': name,
                'age': age,
                'gender': gen,
                'profession': prof,
                'overlooked': overlooked,
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

    # Dedupe by (name, date): API often returns the same obit twice under
    # different URL slugs and tom values (e.g. /business/24do.html and
    # /business/yen-do-65-... on same date). Keep the entry whose tom is the
    # most "canonical" obituary tag, and prefer the longer (slug-based) URL.
    _PREF = {'Obituary (Obit)': 0, 'Obituary': 1, 'Obituary; Biography': 2}
    def _rank(o):
        return (_PREF.get(o.get('tom') or '', 9), -len(o.get('url') or ''))
    seen = {}
    for o in all_obits:
        key = (o.get('name'), o.get('date'))
        # Don't dedupe rows with no parsed name (different events)
        if not key[0]:
            seen[id(o)] = o
            continue
        prev = seen.get(key)
        if prev is None or _rank(o) < _rank(prev):
            seen[key] = o
    deduped = list(seen.values())
    n_dropped = len(all_obits) - len(deduped)
    print(f"Skipped {skipped_corr:,} corrections; deduped {n_dropped:,} same-(name,date) entries")
    all_obits = deduped

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

    with open(OUT_PATH, 'w') as f:
        json.dump(all_obits, f, separators=(',', ':'))
    print(f"\nSaved {OUT_PATH} ({os.path.getsize(OUT_PATH):,} bytes)")


if __name__ == '__main__':
    main()
