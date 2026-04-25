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


def extract_name(headline):
    if not headline: return None
    h = re.sub(r'^Overlooked No More:\s*', '', headline, flags=re.I)
    # Strip leading "'Nickname': " prefixes
    h = re.sub(r'^[\u2018\u201C\'"][^\u2019\u201D\'"]+[\u2019\u201D\'"]\s*[:,]\s*', '', h)
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
    # Pronoun-based first
    if full_text:
        t = ' ' + full_text.lower() + ' '
        he = t.count(' he ') + t.count(' his ') + t.count(' him ')
        she = t.count(' she ') + t.count(' her ') + t.count(' herself ')
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
    skipped = 0

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
