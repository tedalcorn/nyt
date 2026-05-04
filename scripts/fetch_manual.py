"""Walk through missing corrections pages one by one, opening each in Chrome.

For each page:
  1. Chrome opens the URL (you are already logged in).
  2. When the page loads, press Cmd+S → "Web Page, HTML Only"
  3. In the save dialog, navigate to the folder shown below and use
     the filename shown below — then click Save.
  4. Press Enter here to continue to the next page.

The parser only needs the article body, so HTML-only is fine.

Usage:
    python3 scripts/fetch_manual.py          # 2024 missing pages (default)
    python3 scripts/fetch_manual.py 2025     # 2025 missing pages
    python3 scripts/fetch_manual.py 2024 2025

After finishing, run:
    python3 scripts/scrape_corrections.py 2024 2025
    python3 scripts/build_corrections.py
"""
import hashlib, json, os, glob, sys, webbrowser, subprocess

CACHE_DIR = 'cache/corrections'
RAW_DIR   = 'data/raw'
os.makedirs(CACHE_DIR, exist_ok=True)

CACHE_ABS = os.path.abspath(CACHE_DIR)

def slug(url):
    return hashlib.md5(url.encode()).hexdigest()[:16]

def collect_missing(years):
    out = []
    seen = set()
    for f in sorted(glob.glob(os.path.join(RAW_DIR, '*.json'))):
        y = os.path.basename(f)[:4]
        if y not in years:
            continue
        with open(f) as fh:
            docs = json.load(fh)
        for d in docs:
            url = d.get('web_url', '') or ''
            tom = (d.get('type_of_material') or '').strip()
            sec = (d.get('section_name') or '').strip()
            pub = (d.get('pub_date', '') or '')[:10]
            ul  = url.lower()
            if 'corrections-' not in ul or '/no-corrections-' in ul:
                continue
            keep = (tom == 'Correction' or
                    (sec == 'Corrections' and tom not in ('Quote', 'Correction')))
            if not keep or url in seen:
                continue
            seen.add(url)
            path = os.path.join(CACHE_DIR, slug(url) + '.html')
            if os.path.exists(path) and os.path.getsize(path) > 5000:
                continue
            out.append((pub, url))
    out.sort()
    return out

years = set(sys.argv[1:]) if sys.argv[1:] else {'2024'}
missing = collect_missing(years)

if not missing:
    print(f'Nothing missing for year(s) {sorted(years)} — all pages already cached.')
    sys.exit(0)

print(f'\n{len(missing)} pages to fetch manually for year(s) {sorted(years)}')
print(f'\nSave location: {CACHE_ABS}')
print('─' * 60)
print('Workflow for each page:')
print('  1. Chrome opens the URL automatically.')
print('  2. When loaded: Cmd+S → "Web Page, HTML Only"')
print('  3. Navigate to the Save location above.')
print('  4. Paste the filename shown (Cmd+V) → click Save.')
print('  5. Press Enter here to continue.')
print('─' * 60)

# Copy save-folder path to clipboard so user can paste into Finder dialog
try:
    subprocess.run(['pbcopy'], input=CACHE_ABS.encode(), check=True)
    print(f'\n✓ Save folder path copied to clipboard — paste it into the save dialog.')
except Exception:
    pass

ok = skip = 0
for i, (pub, url) in enumerate(missing):
    fname = slug(url) + '.html'
    path  = os.path.join(CACHE_DIR, fname)

    print(f'\n[{i+1}/{len(missing)}] {pub}')
    print(f'  URL : {url}')
    print(f'  File: {fname}')

    webbrowser.open(url)

    input('  → Press Enter after saving the file...')

    if os.path.exists(path) and os.path.getsize(path) > 5000:
        print(f'  ✓ Saved ({os.path.getsize(path):,} bytes)')
        ok += 1
    else:
        print(f'  ✗ File not found at expected path — skipping')
        skip += 1

print(f'\nDone: {ok} saved, {skip} skipped/missing')
if ok:
    print(f'\nNext steps:')
    print(f'  python3 scripts/scrape_corrections.py {" ".join(sorted(years))}')
    print(f'  python3 scripts/build_corrections.py')
