"""Patch dashboard.json with exact annual median word counts.

Replaces the histogram-approximated median_words values (always a multiple of
100, e.g. 1100 or 900) with exact medians computed from article-level data.

Run from the project root:  python3 scripts/patch_medians.py
"""
import json, glob, os, sys

RAW_DIR = 'data/raw'
DASHBOARD = 'data/dashboard.json'

sys.path.insert(0, os.path.dirname(__file__))
from build_data import is_blog_url, is_podcast_article, is_live_url


def exact_median(lst):
    if not lst: return 0
    s = sorted(lst)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) // 2


def collect_medians():
    wc_by_year = {}
    for path in sorted(glob.glob(os.path.join(RAW_DIR, '*.json'))):
        y = os.path.basename(path)[:4]
        with open(path) as fh:
            try:
                docs = json.load(fh)
            except Exception:
                continue
        for d in docs:
            wc = d.get('word_count') or 0
            if wc <= 0:
                continue
            url = d.get('web_url', '') or ''
            section = d.get('section_name', '') or ''
            kicker_str = (d.get('headline') or {}).get('kicker', '') or ''
            tom = d.get('type_of_material', '') or ''
            is_blog = is_blog_url(url)
            is_pod = is_podcast_article(section, url, kicker_str)
            is_live = is_live_url(url) or tom == 'Brief'
            if is_blog or is_pod or is_live:
                continue
            wc_by_year.setdefault(y, []).append(wc)
    return {y: exact_median(lst) for y, lst in wc_by_year.items()}


def main():
    medians = collect_medians()
    print('Computed medians:')
    for y in sorted(medians):
        print(f'  {y}: {medians[y]:,} words')

    with open(DASHBOARD) as fh:
        dash = json.load(fh)

    updated = 0
    for entry in dash.get('articles_per_month', []):
        y = entry['month'][:4]
        if y in medians:
            old = entry.get('median_words', 0)
            new = medians[y]
            if old != new:
                entry['median_words'] = new
                updated += 1

    with open(DASHBOARD, 'w') as fh:
        json.dump(dash, fh, separators=(',', ':'))
    print(f'Updated {updated} month entries in {DASHBOARD}')


if __name__ == '__main__':
    main()
