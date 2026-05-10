"""Import a manually-saved corrections HTML file into the scraper cache.

Use after the audit (audit_corrections_gaps.py) flags a URL as
DATADOME_WALLED — meaning Wayback can never give us the real content.

Workflow:
  1. Open the NYT URL in a browser while signed in to nytimes.com.
  2. Save the page as "Webpage, HTML Only" anywhere on disk.
  3. Run:
       python scripts/import_correction_html.py <URL> <path/to/saved.html>

The script computes the same MD5-based slug that scrape_corrections.py
uses, copies the file into cache/corrections/<slug>.html, and confirms.
The next build_corrections.py run will pick it up automatically.

Sanity checks:
  - File must be ≥ 5,000 bytes (matches scraper's "good content" floor).
  - Bails out if the file appears to be a DataDome challenge page or a
    "Sign in to read this article" gate, since those won't parse and
    you should re-save while logged in.
"""

import hashlib
import os
import shutil
import sys

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(PROJECT_DIR, 'cache', 'corrections')


def main():
    if len(sys.argv) != 3:
        print('Usage: import_correction_html.py <URL> <path/to/saved.html>')
        sys.exit(1)
    url, src_path = sys.argv[1], sys.argv[2]

    if not os.path.exists(src_path):
        print(f'ERROR: source file not found: {src_path}')
        sys.exit(1)

    size = os.path.getsize(src_path)
    if size < 5000:
        print(f'ERROR: file is only {size:,} bytes — too small to be a real article. '
              f'Make sure you saved the full HTML.')
        sys.exit(1)

    with open(src_path, encoding='utf-8', errors='replace') as fh:
        head = fh.read(20_000)

    # Reject obvious non-article saves.
    bad_signals = [
        ('captcha-delivery.com', 'looks like a DataDome challenge page'),
        ('Please verify you are a human', 'looks like a DataDome challenge page'),
        ('Sign in to The New York Times', 'looks like a logged-out paywall page'),
        ('Subscribe to read this article', 'looks like a paywall page'),
    ]
    for needle, why in bad_signals:
        if needle in head:
            print(f'ERROR: {why}. Re-save while logged in to nytimes.com.')
            sys.exit(1)

    # Compute slug exactly as scrape_corrections.py does.
    slug = hashlib.md5(url.encode()).hexdigest()[:16]
    dest_path = os.path.join(CACHE_DIR, slug + '.html')

    os.makedirs(CACHE_DIR, exist_ok=True)
    if os.path.exists(dest_path):
        existing_size = os.path.getsize(dest_path)
        print(f'NOTE: cache file already exists ({existing_size:,} bytes). Overwriting.')

    shutil.copyfile(src_path, dest_path)
    print(f'✓ imported {size:,} bytes')
    print(f'    URL:   {url}')
    print(f'    slug:  {slug}')
    print(f'    cache: {dest_path}')
    print()
    print('Next: re-run build_corrections.py to pick up the new content.')
    print('      python scripts/build_corrections.py')


if __name__ == '__main__':
    main()
