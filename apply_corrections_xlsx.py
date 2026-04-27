#!/usr/bin/env python3
"""Apply manual URL fixes from /Users/tedalcorn/Desktop/2025 corrections that weren't parsed.xlsx.

Patches data/corrections.json by setting inline_url (and ref_date when missing)
on entries matched by text-snippet. Then re-run build_corrections.py.
"""
import json
import shutil
from pathlib import Path

ROOT = Path(__file__).parent
CORR_PATH = ROOT / 'data' / 'corrections.json'

# (text-snippet, inline_url, optional ref_date override or None)
OVERRIDES = [
    ('Because of an editing error, a picture caption accompanying an article on Sunday about international transplant',
     '/2025/12/16/us/organ-transplants-international-patients.html', None),
    ('neighborhood on the outskirts of Berlin',
     '/2025/05/27/realestate/berlin-holocaust-nazis-neighborhood.html', None),
    ('An article on Page 8 this weekend about the FX sitcom',
     '/2025/05/23/arts/television/adults-fx-sitcom-gen-z.html', '2025-05-23'),
    ('An article on Page 4 this weekend about the history of the Places to Go lists',
     '/interactive/2025/travel/places-to-travel-destinations-2025.html', '2025-01-17'),
    ('Brazilian artist Luana Vitra',
     '/2025/06/27/arts/design/brazil-artist-luana-vitra-sculpture-center-queens.html', '2025-06-27'),
]


def main():
    corrs = json.load(open(CORR_PATH))
    patched = 0
    for snip, inline, ref_date in OVERRIDES:
        n = 0
        for c in corrs:
            text = c.get('text') or ''
            if snip in text:
                c['inline_url'] = inline
                if ref_date and not c.get('ref_date'):
                    c['ref_date'] = ref_date
                n += 1
                patched += 1
        print(f'  {n} match(es) for {snip[:50]!r} → {inline}')
    shutil.copy(CORR_PATH, str(CORR_PATH) + '.bak')
    with open(CORR_PATH, 'w') as f:
        json.dump(corrs, f, ensure_ascii=False, indent=2)
    print(f'\nTotal corrections patched: {patched}')
    print(f'Wrote {CORR_PATH}')


if __name__ == '__main__':
    main()
