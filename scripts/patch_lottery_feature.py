"""Surgical patch: peel "Lottery Numbers" daily-results articles out of
state and section analysis, and surface them as a Features entry.

The NYT publishes the daily NY/NJ/CT lottery numbers as a standalone item
(one article per day, headline = "Lottery Numbers", section = "New York",
tagged with all three state geocodes). They were inflating each state's
"Lotteries" subject share — Connecticut's top recurring theme was 19% of
coverage, almost entirely from these daily numbers feeds — and inflating
the New York section's article count.

This script:
  1. For every article whose headline is exactly "Lottery Numbers" and
     section is "New York":
       a. Reassign section to "Today's Paper" (mirrors the Quote of the Day
          handling — keeps the article in the dataset but out of the
          New York section count).
       b. Strip the canonical_states (`st`) array so the article no longer
          counts in any state's article total or subject analysis.
  2. Recompute dashboard.json["features"]["lottery_numbers"] with by-year
     counts and a recent_articles sample, matching the Quote of the Day
     and Letter of Recommendation feature shape.

Idempotent: running again on already-patched data is a no-op.

Run from the project root after build_data.py / patch_beats.py. The
permanent change to detect this pattern at build time is in build_data.py
(SECTION reassignment + state-tag stripping); this surgical script applies
the same fix to the existing on-disk JSON so we don't have to do a full
rebuild.
"""

import json
import os
import glob
from collections import defaultdict

DATA_DIR = 'data'
URL_PREFIX_FULL = 'https://www.nytimes.com'


def is_lottery_numbers(article):
    """Daily NY/NJ/CT lottery results — multiple headline forms over the
    years. Section guard avoids matching unrelated articles in Sports/Opinion
    etc. Patterns observed:
      'Lottery Numbers'                        (2002-2007)
      'Lottery Numbers for [date]'             (2008-2011 era)
      'Lottery Numbers for New York, NJ, CT'   (2009-2013)
      'Lottery Numbers | March 17, 2008'       (occasional pipe variant)
      'Winning Lottery Numbers'                (492 occurrences, dominant)
      'Winning Lottery numbers'                (1, capitalization variant)
      'Winnings Lottery Numbers'               (typo variant)
      'Winning Powerball and Mega Millions'    (Powerball-only days)
      'Powerball and Lottery Numbers'
    """
    sec = article.get('s') or ''
    if sec not in ('New York', "Today's Paper"):
        return False
    h = article.get('h') or ''
    if h.startswith('Lottery Numbers'): return True
    if h.startswith('Winning Lottery'): return True
    if h.startswith('Winnings Lottery'): return True
    if h.startswith('Winning Powerball'): return True
    if h == 'Powerball and Lottery Numbers': return True
    return False


def main():
    article_files = sorted(glob.glob(os.path.join(DATA_DIR, 'articles_*.json')))
    by_year = defaultdict(int)
    recent = []  # list of dicts for the feature's recent_articles
    total_patched = 0

    for fp in article_files:
        with open(fp) as fh:
            arts = json.load(fh)
        changed = 0
        for a in arts:
            if not is_lottery_numbers(a):
                continue
            # Patch: section → Today's Paper, strip state tags
            a['s'] = "Today's Paper"
            a['st'] = []
            changed += 1
            # Tally for the feature
            year = (a.get('d') or '')[:4]
            if year:
                by_year[year] += 1
            url = a.get('u') or ''
            if url.startswith(URL_PREFIX_FULL):
                url = url[len(URL_PREFIX_FULL):]
            recent.append({
                'd': (a.get('d') or '')[:10],
                'h': a.get('h', ''),
                'a': a.get('a', []),
                'w': a.get('w', 0),
                'u': url,
            })
        if changed:
            with open(fp, 'w') as fh:
                json.dump(arts, fh, separators=(',', ':'))
            total_patched += changed
            print(f'  {os.path.basename(fp)}: {changed} articles patched')

    print(f'\nTotal Lottery Numbers articles patched: {total_patched}')

    # Update dashboard.json features
    recent.sort(key=lambda x: x['d'], reverse=True)
    dash_path = os.path.join(DATA_DIR, 'dashboard.json')
    with open(dash_path) as fh:
        dashboard = json.load(fh)
    if 'features' not in dashboard:
        dashboard['features'] = {}
    dashboard['features']['lottery_numbers'] = {
        'by_year': dict(sorted(by_year.items())),
        'recent_articles': recent[:50],
        'total': sum(by_year.values()),
    }
    with open(dash_path, 'w') as fh:
        json.dump(dashboard, fh, separators=(',', ':'))
    print(f'Updated dashboard.json: lottery_numbers feature with '
          f'{dashboard["features"]["lottery_numbers"]["total"]:,} items '
          f'across {len(dashboard["features"]["lottery_numbers"]["by_year"])} years')


if __name__ == '__main__':
    main()
