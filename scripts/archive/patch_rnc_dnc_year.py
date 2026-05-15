"""One-off surgical patch: rewrite Republican/Democratic National Convention
subject tags in data/articles_*.json to include the convention year.

Maps each tagged article's publication date to the closest U.S. election year
(2000, 2004, ..., 2024), with ties broken FORWARD (toward the future
convention — coverage in mid-cycle years is almost always anticipatory).

After patching, the existing year-detection rule in build_state_keywords.py
(any 4-digit year in a tag name → headline event) will automatically reclassify
these as event-driven, removing them from the 'recurring' theme analysis used
by the state cards and state map.

This is a one-off because build_data.py is also being updated to apply the
same rewrite to every article it processes — future nightly rebuilds won't
need this patch.

Output JSON uses separators=(',', ':') to match the compact format the live
site loads.
"""

import json
import os
from glob import glob

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_DIR, 'data')
ELECTION_YEARS = [2000, 2004, 2008, 2012, 2016, 2020, 2024]
RENAMES = {
    'Republican National Convention': 'Republican National Convention ({})',
    'Democratic National Convention': 'Democratic National Convention ({})',
}


def closest_election_year(article_year):
    """Closest election year to the given pub year; ties broken forward."""
    best = ELECTION_YEARS[0]
    for ey in ELECTION_YEARS[1:]:
        d_new = abs(ey - article_year)
        d_old = abs(best - article_year)
        if d_new < d_old or (d_new == d_old and ey > best):
            best = ey
    return best


def main():
    modified_files = 0
    modified_articles = 0
    paths = sorted(glob(os.path.join(DATA_DIR, 'articles_*.json')))
    for fpath in paths:
        with open(fpath) as f:
            articles = json.load(f)
        changed = False
        for a in articles:
            subjects = a.get('sb') or []
            if not any(s in RENAMES for s in subjects):
                continue
            date = a.get('d', '')
            try:
                article_year = int(date[:4])
            except (ValueError, TypeError):
                continue
            conv_year = closest_election_year(article_year)
            new_subjects = []
            for s in subjects:
                if s in RENAMES:
                    new_subjects.append(RENAMES[s].format(conv_year))
                else:
                    new_subjects.append(s)
            a['sb'] = new_subjects
            modified_articles += 1
            changed = True
        if changed:
            with open(fpath, 'w') as f:
                json.dump(articles, f, separators=(',', ':'))
            modified_files += 1
            print(f'  patched: {os.path.basename(fpath)}')

    print(f'\nModified {modified_articles} articles in {modified_files} files.')


if __name__ == '__main__':
    main()
