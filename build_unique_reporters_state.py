"""Build unique_reporters_by_state.json from raw API files."""
import json, glob, re
from collections import defaultdict

ABBREV = {
    'Ala':'Alabama','Ariz':'Arizona','Ark':'Arkansas','Calif':'California',
    'Colo':'Colorado','Conn':'Connecticut','Del':'Delaware','Fla':'Florida',
    'Ga':'Georgia','Idaho':'Idaho','Ill':'Illinois','Ind':'Indiana','Iowa':'Iowa',
    'Kan':'Kansas','Ky':'Kentucky','La':'Louisiana','Me':'Maine','Md':'Maryland',
    'Mass':'Massachusetts','Mich':'Michigan','Minn':'Minnesota','Miss':'Mississippi',
    'Mo':'Missouri','Mont':'Montana','Neb':'Nebraska','Nev':'Nevada',
    'NH':'New Hampshire','NJ':'New Jersey','NM':'New Mexico','NY':'New York',
    'NC':'North Carolina','ND':'North Dakota','Ohio':'Ohio','Okla':'Oklahoma',
    'Ore':'Oregon','Pa':'Pennsylvania','RI':'Rhode Island','SC':'South Carolina',
    'SD':'South Dakota','Tenn':'Tennessee','Tex':'Texas','Utah':'Utah','Vt':'Vermont',
    'Va':'Virginia','Wash':'Washington','WVa':'West Virginia','Wis':'Wisconsin',
    'Wyo':'Wyoming','DC':'D.C.','Alaska':'Alaska','Hawaii':'Hawaii',
}
US_STATES = set(ABBREV.values())
NYC = {'Manhattan (NYC)','Queens (NYC)','Brooklyn (NYC)','Bronx (NYC)','Staten Island (NYC)','New York City'}

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

data = defaultdict(lambda: defaultdict(set))

files = sorted(glob.glob('data/raw/*.json'))
print(f'Processing {len(files)} files...')
for i, f in enumerate(files):
    import os
    basename = os.path.basename(f)  # e.g. "2022-06.json"
    year = basename[:4]
    if not year.isdigit():
        continue
    with open(f) as fh:
        docs = json.load(fh)
    for d in docs:
        sec = d.get('section_name', '')
        if sec not in ('U.S.', 'New York'):
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
                data[state][year].add(name)
    if (i + 1) % 50 == 0:
        print(f'  [{i+1}/{len(files)}]  states so far: {len(data)}')

result = {
    state: {yr: len(reporters) for yr, reporters in years.items()}
    for state, years in data.items()
    if len(years) >= 3
}

with open('data/unique_reporters_by_state.json', 'w') as f:
    json.dump(result, f, separators=(',', ':'))

print(f'Done. States: {len(result)}')
print('California:', dict(list(result.get('California', {}).items())[-5:]))
