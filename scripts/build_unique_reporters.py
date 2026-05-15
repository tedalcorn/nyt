"""Build unique_reporters_by_section.json, _by_state.json, _by_country.json.

Counts distinct reporter bylines per section / state / country per year.
These power the 'Unique reporters per year' toggle in the Sections, States,
and World popups.

All three counts read from articles_*.json (the compact, per-year files
produced by build_data.py) so they share the same canonical author names —
the ones build_data.py produced after applying author_overrides and prefix
deduplication. A previous version of the state pass read raw API byline
fields directly, which produced subtly different reporter counts than the
section/country passes.

Run after build_data.py.
"""
import json, glob, os
from collections import defaultdict

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')

SECTION_RENAMES = {
    'Fashion & Style': 'Style', 'Fashion': 'Style', 'Business Day': 'Business',
    'Gameplay': 'Crosswords & Games', 'Book Review': 'Books',
    'Great Homes & Destinations': 'Real Estate', 'At Home': 'Style',
    "Critic's Choice": 'Arts', 'Week in Review': 'Sunday Review',
    'en Español': 'En español',
}
SECTION_EXCLUDED = {
    'Archives', "Today's Paper", 'Corrections', 'Multimedia/Photos',
    'The Learning Network', 'T Magazine', 'Briefing', 'Smarter Living',
    'Booming', 'UrbanEye', 'Guide',
}
# Bylines to exclude when counting "unique reporters". These are institutional
# bylines, not individual reporters.
INSTITUTIONAL = {
    'Reuters', 'AP', 'Agence France-Presse', 'The Associated Press',
    'Bloomberg News',
}


def _is_real_reporter(name):
    """Author should be counted toward unique-reporter tallies."""
    if not name or len(name) <= 3:
        return False
    if name in INSTITUTIONAL:
        return False
    if name.startswith('The '):
        return False
    return True


def build_by_section(article_files):
    """{section: {year: count_of_distinct_reporters}}"""
    sec_data = defaultdict(lambda: defaultdict(set))
    for f in article_files:
        year = os.path.basename(f)[-9:-5]
        with open(f) as fh:
            arts = json.load(fh)
        for a in arts:
            sec = SECTION_RENAMES.get(a.get('s', ''), a.get('s', ''))
            if not sec or sec in SECTION_EXCLUDED:
                continue
            for author in (a.get('a') or []):
                if _is_real_reporter(author):
                    sec_data[sec][year].add(author)
    return {
        sec: {yr: len(reporters) for yr, reporters in years.items()}
        for sec, years in sec_data.items()
        if len(years) >= 3
    }


def build_by_state(article_files):
    """{state: {year: count_of_distinct_reporters}}.

    Uses 'st' (canonical_states) precomputed by build_data.py, so state names
    are already canonical. Article-level filtering: build_data.py only sets
    'st' for U.S./New York-section articles AND skips lottery articles —
    so this loop doesn't need to re-check section/lottery.
    """
    state_data = defaultdict(lambda: defaultdict(set))
    for f in article_files:
        year = os.path.basename(f)[-9:-5]
        with open(f) as fh:
            arts = json.load(fh)
        for a in arts:
            states = a.get('st') or []
            if not states:
                continue
            for author in (a.get('a') or []):
                if not _is_real_reporter(author):
                    continue
                for state in states:
                    state_data[state][year].add(author)
    return {
        state: {yr: len(reporters) for yr, reporters in years.items()}
        for state, years in state_data.items()
        if len(years) >= 3
    }


def build_by_country(article_files):
    """{country: {year: count_of_distinct_reporters}} for World-section articles."""
    country_data = defaultdict(lambda: defaultdict(set))
    for f in article_files:
        year = os.path.basename(f)[-9:-5]
        with open(f) as fh:
            arts = json.load(fh)
        for a in arts:
            if a.get('s') != 'World':
                continue
            locs = a.get('gn') or a.get('g') or []
            for author in (a.get('a') or []):
                if not _is_real_reporter(author):
                    continue
                for loc in locs:
                    country_data[loc][year].add(author)
    return {
        loc: {yr: len(reporters) for yr, reporters in years.items()}
        for loc, years in country_data.items()
        if len(years) >= 3 and max(len(r) for r in years.values()) >= 3
    }


def main():
    article_files = sorted(glob.glob(os.path.join(DATA_DIR, 'articles_*.json')))
    print(f'Scanning {len(article_files)} per-year article files...')

    print('Building unique reporters by section...')
    sec_result = build_by_section(article_files)
    out_path = os.path.join(DATA_DIR, 'unique_reporters_by_section.json')
    with open(out_path, 'w') as f:
        json.dump(sec_result, f, separators=(',', ':'))
    print(f'  → {len(sec_result)} sections → {os.path.basename(out_path)}')

    print('Building unique reporters by state...')
    state_result = build_by_state(article_files)
    out_path = os.path.join(DATA_DIR, 'unique_reporters_by_state.json')
    with open(out_path, 'w') as f:
        json.dump(state_result, f, separators=(',', ':'))
    print(f'  → {len(state_result)} states → {os.path.basename(out_path)}')

    print('Building unique reporters by country...')
    country_result = build_by_country(article_files)
    out_path = os.path.join(DATA_DIR, 'unique_reporters_by_country.json')
    with open(out_path, 'w') as f:
        json.dump(country_result, f, separators=(',', ':'))
    print(f'  → {len(country_result)} countries → {os.path.basename(out_path)}')


if __name__ == '__main__':
    main()
