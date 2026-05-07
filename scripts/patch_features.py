"""Surgical patch: add a batch of standing-feature entries to
dashboard.json["features"] without re-running build_data.py.

Each entry has the same shape as the existing weddings / quote_of_the_day
features: {by_year, recent_articles, total, top_authors?}. Detection is by
exact-match or starts-with on the headline (per the standing-feature audit
in tag_config.json).

Run from the project root:
    python3 scripts/patch_features.py
"""

import json
import os
import glob
from collections import Counter, defaultdict

DATA_DIR = 'data'
URL_PREFIX_FULL = 'https://www.nytimes.com'

# Each feature definition gives a key (matches FEATURE_META in index.html), a
# label/desc/period for the registry, and a `match(headline) -> bool` test.
FEATURES = [
    {
        'key': 'letters_to_editor',
        'match': lambda h: h == 'Letters to the Editor',
    },
    {
        'key': 'on_the_market',
        'match': lambda h: h.startswith('On the Market in'),
    },
    {
        'key': 'metropolitan_diary',
        'match': lambda h: h == 'Metropolitan Diary',
    },
    {
        'key': 'boldface_names',
        'match': lambda h: h == 'BOLDFACE NAMES',
    },
    {
        'key': 'names_of_the_dead',
        'match': lambda h: h == 'Names of the Dead',
    },
    {
        'key': 'coronavirus_briefing',
        'match': lambda h: h.startswith('Coronavirus Briefing'),
    },
    {
        'key': 'arts_briefly',
        'match': lambda h: h == 'Arts, Briefly',
    },
]


def relative_url(u):
    if u and u.startswith(URL_PREFIX_FULL):
        return u[len(URL_PREFIX_FULL):]
    return u or ''


def main():
    article_files = sorted(glob.glob(os.path.join(DATA_DIR, 'articles_*.json')))
    by_year = {f['key']: defaultdict(int) for f in FEATURES}
    recent = {f['key']: [] for f in FEATURES}
    authors = {f['key']: Counter() for f in FEATURES}

    for fp in article_files:
        with open(fp) as fh:
            arts = json.load(fh)
        for a in arts:
            h = a.get('h') or ''
            if not h:
                continue
            for feat in FEATURES:
                if not feat['match'](h):
                    continue
                key = feat['key']
                year = (a.get('d') or '')[:4]
                if year:
                    by_year[key][year] += 1
                for au in (a.get('a') or []):
                    authors[key][au] += 1
                recent[key].append({
                    'd': (a.get('d') or '')[:10],
                    'h': h,
                    'a': a.get('a', []),
                    'w': a.get('w', 0),
                    'u': relative_url(a.get('u')),
                })
                break  # each article matches at most one feature

    # Build feature payloads
    dash_path = os.path.join(DATA_DIR, 'dashboard.json')
    with open(dash_path) as fh:
        dashboard = json.load(fh)
    if 'features' not in dashboard:
        dashboard['features'] = {}

    for feat in FEATURES:
        key = feat['key']
        recent[key].sort(key=lambda x: x['d'], reverse=True)
        dashboard['features'][key] = {
            'by_year': dict(sorted(by_year[key].items())),
            'recent_articles': recent[key][:50],
            'top_authors': [
                {'name': n, 'count': c} for n, c in authors[key].most_common(8)
            ],
            'total': sum(by_year[key].values()),
        }
        print(f"  {key:25s}  {dashboard['features'][key]['total']:>6,} items "
              f"across {len(dashboard['features'][key]['by_year'])} years")

    with open(dash_path, 'w') as fh:
        json.dump(dashboard, fh, separators=(',', ':'))
    print(f'\nUpdated {dash_path}')


if __name__ == '__main__':
    main()
