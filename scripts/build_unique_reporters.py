"""Build unique_reporters_by_section.json and unique_reporters_by_state.json.

Counts distinct reporter bylines per section/state per year. These power the
'Unique reporters per year' toggle in the Sections and States popups.

Run after build_data.py (reads articles_*.json and data/raw/*.json).
"""
import json, glob, os, re
from collections import defaultdict

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')

# ── Unique reporters by section ───────────────────────────────────────────────
print('Building unique reporters by section...')

SECTION_RENAMES = {
    'Fashion & Style': 'Style', 'Fashion': 'Style', 'Business Day': 'Business',
    'Gameplay': 'Crosswords & Games', 'Book Review': 'Books',
    'Great Homes & Destinations': 'Real Estate', 'At Home': 'Style',
    "Critic's Choice": 'Arts', 'Week in Review': 'Sunday Review',
    'en Español': 'En español',
}
EXCLUDED = {
    'Archives', "Today's Paper", 'Corrections', 'Multimedia/Photos',
    'The Learning Network', 'T Magazine', 'Briefing', 'Smarter Living',
    'Booming', 'UrbanEye', 'Guide',
}

sec_data = defaultdict(lambda: defaultdict(set))
for f in sorted(glob.glob(os.path.join(DATA_DIR, 'articles_*.json'))):
    year = os.path.basename(f)[-9:-5]
    with open(f) as fh:
        arts = json.load(fh)
    for a in arts:
        sec = SECTION_RENAMES.get(a.get('s', ''), a.get('s', ''))
        if not sec or sec in EXCLUDED:
            continue
        for author in (a.get('a') or []):
            if author and not author.startswith('The ') and author not in ('Reuters', 'AP', 'Agence France-Presse'):
                sec_data[sec][year].add(author)

sec_result = {
    sec: {yr: len(reporters) for yr, reporters in years.items()}
    for sec, years in sec_data.items()
    if len(years) >= 3
}
out_path = os.path.join(DATA_DIR, 'unique_reporters_by_section.json')
with open(out_path, 'w') as f:
    json.dump(sec_result, f, separators=(',', ':'))
print(f'  → {len(sec_result)} sections → {os.path.basename(out_path)}')


# ── Unique reporters by state ─────────────────────────────────────────────────
print('Building unique reporters by state...')

ABBREV = {
    'Ala': 'Alabama', 'Ariz': 'Arizona', 'Ark': 'Arkansas', 'Calif': 'California',
    'Colo': 'Colorado', 'Conn': 'Connecticut', 'Del': 'Delaware', 'Fla': 'Florida',
    'Ga': 'Georgia', 'Idaho': 'Idaho', 'Ill': 'Illinois', 'Ind': 'Indiana',
    'Iowa': 'Iowa', 'Kan': 'Kansas', 'Ky': 'Kentucky', 'La': 'Louisiana',
    'Me': 'Maine', 'Md': 'Maryland', 'Mass': 'Massachusetts', 'Mich': 'Michigan',
    'Minn': 'Minnesota', 'Miss': 'Mississippi', 'Mo': 'Missouri', 'Mont': 'Montana',
    'Neb': 'Nebraska', 'Nev': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
    'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota',
    'Ohio': 'Ohio', 'Okla': 'Oklahoma', 'Ore': 'Oregon', 'Pa': 'Pennsylvania',
    'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota',
    'Tenn': 'Tennessee', 'Tex': 'Texas', 'Utah': 'Utah', 'Vt': 'Vermont',
    'Va': 'Virginia', 'Wash': 'Washington', 'WVa': 'West Virginia',
    'Wis': 'Wisconsin', 'Wyo': 'Wyoming', 'DC': 'D.C.',
    'Alaska': 'Alaska', 'Hawaii': 'Hawaii',
}
US_STATES = set(ABBREV.values())
NYC = {'Manhattan (NYC)', 'Queens (NYC)', 'Brooklyn (NYC)', 'Bronx (NYC)',
       'Staten Island (NYC)', 'New York City'}


def loc_to_state(loc):
    if not loc:
        return None
    loc = loc.strip()
    for s in US_STATES:
        if s.upper() == loc.upper():
            return s
    if loc in NYC:
        return 'New York'
    m = re.search(r'\(([^)]+)\)', loc)
    if m:
        return ABBREV.get(m.group(1))
    return None


def parse_byline(byline_obj):
    if not byline_obj or not isinstance(byline_obj, dict):
        return []
    orig = (byline_obj.get('original') or '').strip()
    if not orig:
        return []
    orig = re.sub(r'^By\s+', '', orig, flags=re.I)
    names = re.split(r'\s+and\s+|\s*,\s+', orig)
    return [n.strip() for n in names if n.strip() and len(n.strip()) > 3]


state_data = defaultdict(lambda: defaultdict(set))
raw_files = sorted(glob.glob(os.path.join(DATA_DIR, 'raw', '*.json')))
print(f'  Processing {len(raw_files)} raw files...')
for f in raw_files:
    basename = os.path.basename(f)
    year = basename[:4]
    if not year.isdigit():
        continue
    with open(f) as fh:
        docs = json.load(fh)
    for d in docs:
        if d.get('section_name', '') not in ('U.S.', 'New York'):
            continue
        names = parse_byline(d.get('byline'))
        if not names:
            continue
        states = set()
        for kw in (d.get('keywords') or []):
            if kw.get('name') in ('glocations', 'Location'):
                state = loc_to_state(kw.get('value', ''))
                if state:
                    states.add(state)
        for state in states:
            for name in names:
                state_data[state][year].add(name)

state_result = {
    state: {yr: len(reporters) for yr, reporters in years.items()}
    for state, years in state_data.items()
    if len(years) >= 3
}
out_path = os.path.join(DATA_DIR, 'unique_reporters_by_state.json')
with open(out_path, 'w') as f:
    json.dump(state_result, f, separators=(',', ':'))
print(f'  → {len(state_result)} states → {os.path.basename(out_path)}')
print('Done.')
