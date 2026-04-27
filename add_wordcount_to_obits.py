#!/usr/bin/env python3
"""One-shot: enrich data/obituaries.json with word_count from articles_*.json.

Surgical regenerator (no full pipeline rerun). Also patches build_obituaries.py
so future rebuilds keep the field.
"""
import json
import glob
import shutil
from pathlib import Path

ROOT = Path(__file__).parent
OBIT_JSON = ROOT / 'data' / 'obituaries.json'


def main():
    by_url = {}
    for f in sorted(glob.glob(str(ROOT / 'data' / 'articles_*.json'))):
        for a in json.load(open(f)):
            u = a.get('u', '')
            if u:
                by_url[u] = a.get('w', 0) or 0

    obs = json.load(open(OBIT_JSON))
    found = 0
    missing = 0
    for o in obs:
        u = o.get('url', '')
        if u in by_url:
            o['word_count'] = by_url[u]
            found += 1
        else:
            o['word_count'] = 0
            missing += 1
    print(f'  matched: {found}, missing: {missing}')

    shutil.copy(OBIT_JSON, str(OBIT_JSON) + '.bak2')
    with open(OBIT_JSON, 'w') as f:
        json.dump(obs, f, ensure_ascii=False, indent=2)
    print(f'  Wrote {OBIT_JSON}')


if __name__ == '__main__':
    main()
