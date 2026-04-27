"""Per-reporter and per-section/year article counts, scoped to the universe
the corrections viz needs.

Output: data/corrections_denominators.json
{
  "years": [2017, 2018, ..., 2025],
  "section_year_counts": {section: {year: count}},  // mirrors section_trends but
                                                    // restricted to corrections era
  "reporter_year_counts": {reporter: {year: count}},  // only reporters who appear
                                                      // in corrections_matched.json
  "overall_year_counts": {year: count}              // for the "overall" view
}

Idempotent. Read-only on inputs.
"""
import json, glob
from collections import defaultdict

OUT = 'data/corrections_denominators.json'


def main():
    with open('data/corrections_matched.json') as f:
        cm = json.load(f)

    # Reporters who have any matched correction → restrict denominator universe
    reporters_with_corrections = set()
    for c in cm:
        if not c.get('match_url'):
            continue
        for au in (c.get('match_authors') or []):
            if au:
                reporters_with_corrections.add(au)
    print(f'Reporters with at least one correction: {len(reporters_with_corrections):,}')

    # Year window — corrections data spans 2016-12 → present, but page_date determines
    # the corrections era. Use 2017-present.
    years = [str(y) for y in range(2017, 2027)]

    section_year = defaultdict(lambda: defaultdict(int))
    reporter_year = defaultdict(lambda: defaultdict(int))
    overall_year = defaultdict(int)

    for f in sorted(glob.glob('data/articles_*.json')):
        # Only need 2017+
        y = f.split('articles_')[1].split('.')[0]
        if y not in years:
            continue
        with open(f) as fh:
            arts = json.load(fh)
        for a in arts:
            d = a.get('d') or ''
            yr = d[:4] if d else y
            if yr not in years:
                continue
            sec = a.get('s') or ''
            authors = a.get('a') or []
            overall_year[yr] += 1
            if sec:
                section_year[sec][yr] += 1
            for au in authors:
                if au in reporters_with_corrections:
                    reporter_year[au][yr] += 1
        print(f'  scanned {f}')

    out = {
        'years': years,
        'section_year_counts': {s: dict(yc) for s, yc in section_year.items()},
        'reporter_year_counts': {r: dict(yc) for r, yc in reporter_year.items()},
        'overall_year_counts': dict(overall_year),
    }

    with open(OUT, 'w') as f:
        json.dump(out, f, separators=(',', ':'))
    import os
    print(f'\nWrote {OUT} ({os.path.getsize(OUT):,} bytes)')
    print(f'  sections: {len(out["section_year_counts"])}')
    print(f'  reporters: {len(out["reporter_year_counts"])}')
    print(f'  overall by year: {dict(sorted(overall_year.items()))}')


if __name__ == '__main__':
    main()
