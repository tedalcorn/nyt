"""
Generate lean tracker_YYYY.json files from existing articles_YYYY.json files.
These contain only the 4 fields the headline frequency chart needs:
  h  = headline
  m  = year_month
  ph = print headline (omitted if empty)
  fp = 1 if front page (print_section='A' and print_page='1'), omitted otherwise

Run whenever articles_YYYY.json files change (e.g. after build_data.py).
"""

import json, os, time

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

total_full = 0
total_tracker = 0

for year in range(2000, 2026):
    src = os.path.join(DATA_DIR, f'articles_{year}.json')
    if not os.path.exists(src):
        continue

    with open(src) as f:
        arts = json.load(f)

    tracker = []
    for a in arts:
        t = {'h': a['h'], 'm': a['m']}
        if a.get('ph'):
            t['ph'] = a['ph']
        if a.get('ps') == 'A' and str(a.get('pp', '')) == '1':
            t['fp'] = 1
        tracker.append(t)

    dst = os.path.join(DATA_DIR, f'tracker_{year}.json')
    with open(dst, 'w') as f:
        json.dump(tracker, f, separators=(',', ':'))

    full_mb  = os.path.getsize(src) / 1e6
    track_mb = os.path.getsize(dst) / 1e6
    total_full    += full_mb
    total_tracker += track_mb
    print(f'  {year}: {len(arts):,} articles  full={full_mb:.0f}MB  tracker={track_mb:.0f}MB')

print(f'\nDone. Full total: {total_full:.0f}MB → Tracker total: {total_tracker:.0f}MB '
      f'({total_tracker/total_full*100:.0f}% of original)')
