"""Surgical regenerator for the `world_coverage` block in dashboard.json.

Re-reads all raw NYT Archive dumps, applies the SAME location normalization
as build_data.py with a single difference: Greenland is no longer collapsed
into Denmark. (Greenland is a Danish autonomous territory, but it has its
own GeoJSON polygon in our world map and the user wants it surfaced.)

Why this exists: a full build_data.py run takes several minutes and rebuilds
~30MB of artifacts. Just the geo aggregation is ~30 seconds, so we patch
dashboard.json in place rather than triggering a full rebuild.
"""
import json
import glob
import os
from collections import Counter, defaultdict
from urllib.parse import urlparse


# -- Mirror of build_data.py's _normalize_loc, with Greenland un-collapsed.
EXPLICIT = {
    # Cities and territories that map to a parent country (Greenland removed!)
    "Mayotte (Comoro Islands)": "France",
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
    "Faroe Islands": "Denmark",
    "Canary Islands": "Spain",
    "Azores Islands": "Portugal",
    "Andaman Islands": "India",
    "Galapagos Islands": "Ecuador",
    "Easter Island": "Chile",
    "Grand Bahama Island": "Bahamas",
    "Bahama Islands": "Bahamas",
    "ABACO ISLANDS (BAHAMAS)": "Bahamas",
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
    "Dili (East Timor)": "East Timor",
    "Bangui (Central African Republic)": "Central African Republic",
    "Nuuk (Greenland)": "Greenland",  # was Denmark
    "ANTARCTIC REGIONS": "Antarctica",
    "Antarctic Regions": "Antarctica",
    "ANTARCTICA": "Antarctica",
    "St Martin (Caribbean)": "Caribbean Area",
    "Brixton (London, England)": "Great Britain",
    "Sevnica (Slovenia)": "Slovenia",
    "Yellowknife (Northwest Territories)": "Canada",
}

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
    "Beijing": "China", "NYC": "United States",
    # US states (keep existing list; relevant only for "City (Abbr)" patterns)
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
    "Eng": "Great Britain", "England": "Great Britain",
    "Ger": "Germany", "Germany": "Germany",
    "Mex": "Mexico", "Gaza": "Gaza Strip", "Indian State": "India",
    "West Indies": "Caribbean Area", "Congo": "Democratic Republic of Congo",
    "Bahamas": "Bahamas", "Tasmania": "Australia",
    "United Kingdom": "Great Britain",
    # NOTE: Greenland intentionally NOT in PARENT_MAP — it stays as itself.
    "Antarctica": "Antarctica",
    "ANTARCTICA": "Antarctica",
    "Caribbean": "Caribbean Area",
    "Portugal": "Portugal", "Paraguay": "Paraguay", "Bolivia": "Bolivia",
    "Croatia": "Croatia", "Slovenia": "Slovenia", "Slovakia": "Slovakia",
    "Latvia": "Latvia", "Estonia": "Estonia", "Lithuania": "Lithuania",
    "Albania": "Albania", "Bulgaria": "Bulgaria", "Finland": "Finland",
    "Iceland": "Iceland", "Uruguay": "Uruguay", "Chile": "Chile",
    "Ecuador": "Ecuador", "Guatemala": "Guatemala", "Honduras": "Honduras",
    "Nicaragua": "Nicaragua", "Dominican Republic": "Dominican Republic",
}

import re
RE_CITY_PARENT = re.compile(r'^[A-Z][\w\s.\'\-]*\s\(([^)]+)\)$')
RE_BRACKET_PARENT = re.compile(r'^[A-Z][\w\s.\'\-]+,\s+([A-Z][\w\s.\'\-]+)$')

# Drop tokens that are too generic (oceans, regions) — mirror build_data.py
DROP = {
    "World", "Americas", "Asia", "Europe", "Africa", "Middle East", "Oceania",
    "Western Hemisphere", "Eastern Hemisphere", "South America", "North America",
    "Central America", "Latin America", "Sub-Saharan Africa", "North Africa",
    "Southeast Asia", "South Asia", "Central Asia", "East Asia", "West Africa",
    "Central Africa", "East Africa", "Southern Africa", "Eastern Europe",
    "Western Europe", "Northern Europe", "Southern Europe", "Balkans",
    "Caucasus", "Scandinavia", "Iberian Peninsula", "Mediterranean Region",
    "Persian Gulf", "Arabian Peninsula", "Caribbean Area", "Caribbean Sea",
    "Atlantic Ocean", "Pacific Ocean", "Indian Ocean", "Arctic Ocean",
    "Mediterranean Sea", "Black Sea", "Baltic Sea", "Red Sea", "Caspian Sea",
}


def normalize_loc(loc):
    if not loc or loc in DROP:
        return None
    # Strip trailing whitespace / standardize
    loc = loc.strip()
    if loc in EXPLICIT:
        return EXPLICIT[loc]
    # "City (Parent)" pattern
    m = RE_CITY_PARENT.match(loc)
    if m:
        parent = m.group(1).strip()
        if parent in PARENT_MAP:
            return PARENT_MAP[parent]
    if loc in PARENT_MAP:
        return PARENT_MAP[loc]
    return loc


# US-section detection — used by build_data.py to reject US-internal articles
# that lack a national-section label. Skip article if section==U.S./New York
# and only US states are tagged. We'll trust the "World" filter from
# build_data.py: only count articles where section=='World'.

def main():
    print("Reading 315 raw monthly dumps...")
    glocation_year = defaultdict(lambda: defaultdict(int))
    glocation_total = Counter()
    region_year = defaultdict(lambda: defaultdict(int))
    raw_files = sorted(glob.glob('data/raw/*.json'))
    for i, f in enumerate(raw_files, 1):
        if i % 50 == 0:
            print(f"  {i}/{len(raw_files)} ({f})")
        d = json.load(open(f))
        docs = d if isinstance(d, list) else d.get('docs') or d.get('response', {}).get('docs', [])
        for art in docs:
            section = (art.get('section_name') or art.get('section') or '').strip()
            if section != 'World':
                continue
            pub = (art.get('pub_date') or '')[:10]
            year = pub[:4] if pub else ''
            if not year: continue
            sub = (art.get('subsection_name') or art.get('subsection') or '').strip()
            if sub:
                region_year[sub][year] += 1
            for k in art.get('keywords') or []:
                kn = k.get('name') or ''
                if kn in ('glocations', 'Location'):
                    raw_loc = k.get('value') or ''
                    loc = normalize_loc(raw_loc)
                    if loc:
                        glocation_year[loc][year] += 1
                        glocation_total[loc] += 1

    # Top locations: ≥5 articles total
    top_locs = [loc for loc, cnt in glocation_total.most_common() if cnt >= 5]

    new_world_coverage = {
        'locations': top_locs,
        'location_trends': {loc: dict(glocation_year[loc]) for loc in top_locs},
        'region_trends': {r: dict(region_year[r]) for r in sorted(region_year.keys())},
        'years': sorted({y for trends in glocation_year.values() for y in trends.keys()}),
    }

    # Patch dashboard.json — preserve all other fields, replace world_coverage
    print("\nLoading dashboard.json...")
    dashboard_path = 'data/dashboard.json'
    with open(dashboard_path) as f:
        dash = json.load(f)
    old = dash.get('world_coverage', {})
    print(f"  old world_coverage: {len(old.get('locations', []))} locations")
    print(f"  new world_coverage: {len(top_locs)} locations")
    # Preserve blog_location_trends if present (we don't recompute it here)
    if 'blog_location_trends' in old:
        new_world_coverage['blog_location_trends'] = old['blog_location_trends']
    dash['world_coverage'] = new_world_coverage
    with open(dashboard_path, 'w') as f:
        json.dump(dash, f, separators=(',', ':'))
    sz = os.path.getsize(dashboard_path)
    print(f"  patched dashboard.json ({sz:,} bytes)")
    print()
    print(f"Greenland 2025: {glocation_year['Greenland'].get('2025', 0)}")
    print(f"Greenland total: {glocation_total['Greenland']}")
    print(f"Denmark 2025: {glocation_year['Denmark'].get('2025', 0)}")
    print(f"Denmark total: {glocation_total['Denmark']}")
    print(f"Antarctica total: {glocation_total['Antarctica']}")
    print(f"Antarctica 2025: {glocation_year['Antarctica'].get('2025', 0)}")


if __name__ == '__main__':
    main()
