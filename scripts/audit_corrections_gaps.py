"""Audit corrections-scraping coverage for the NYT Archive corpus.

For every URL the API tags as a Correction (or section=Corrections, non-Quote),
verify we have a usable HTML snapshot in cache/corrections/. For each missing
URL, query Wayback's CDX index to classify *why* — so you can tell which
gaps will fill themselves on a future run vs. which need manual intervention.

Output: data/corrections_gap_report.md (markdown, easy to scan) + a JSON
sidecar with the same data programmatically.

Run from the project root:
  python scripts/audit_corrections_gaps.py            # default: 2014+ (actual scraper era)
  python scripts/audit_corrections_gaps.py 2024 2025  # specific years
  python scripts/audit_corrections_gaps.py --since 2020
  python scripts/audit_corrections_gaps.py --all-time # include pre-2014 (informational)

Pre-2014 URLs are intentionally excluded from the default scope: the
NYT corrections-publishing pattern shifted to /pageoneplus/ in late
2013, and the scraper was designed for that era forward. Earlier URLs
that happen to match "corrections-" are a different beast and not
in our coverage promise.

Why this script exists: trends in corrections-per-section are extremely
sensitive to coverage gaps. A year with 5% of its corrections URLs
unscraped looks like a 5% drop in the corrections rate — but it's an
artifact, not a real signal. This audit surfaces every gap explicitly.

Three classifications for each missing URL:
  WAITING         — Wayback hasn't crawled this URL yet. Common for very
                     recent dates. Will likely backfill within weeks. Just
                     re-run the scraper periodically.
  DATADOME_WALLED — Wayback HAS snapshots but every snapshot is a 403 from
                     NYT's DataDome bot-protection. Will not improve unless
                     NYT removes DataDome or Wayback re-crawls from a
                     different vantage point. Needs MANUAL SAVE.
  HAS_200_RETRY   — CDX shows a 200-status snapshot exists but our scraper
                     somehow didn't grab it. Probably a transient failure;
                     re-run the scraper to retry.

For DATADOME_WALLED URLs the report includes a "manual import" recipe.
"""

import argparse
import hashlib
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import urllib.error
from collections import defaultdict
from glob import glob

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_DIR, 'data', 'raw')
CACHE_DIR = os.path.join(PROJECT_DIR, 'cache', 'corrections')
REPORT_MD = os.path.join(PROJECT_DIR, 'data', 'corrections_gap_report.md')
REPORT_JSON = os.path.join(PROJECT_DIR, 'data', 'corrections_gap_report.json')

UA = {'User-Agent': 'Mozilla/5.0 (compatible; CorrectionsAudit/1.0)'}


def slug(url):
    return hashlib.md5(url.encode()).hexdigest()[:16]


def collect_urls(years=None):
    """Mirror scrape_corrections.collect_urls so we audit the same set."""
    out = []
    seen = set()
    for f in sorted(glob(os.path.join(RAW_DIR, '*.json'))):
        y = os.path.basename(f)[:4]
        if years is not None and y not in years:
            continue
        try:
            with open(f) as fh:
                docs = json.load(fh)
        except Exception:
            continue
        for d in docs:
            url = d.get('web_url', '') or ''
            if not url:
                continue
            tom = (d.get('type_of_material') or '').strip()
            sec = (d.get('section_name') or '').strip()
            ul = url.lower()
            if 'corrections-' not in ul:
                continue
            if '/no-corrections-' in ul:
                continue
            keep = (
                tom == 'Correction'
                or (sec == 'Corrections' and tom != 'Quote' and tom != 'Correction')
            )
            if not keep:
                continue
            if url in seen:
                continue
            seen.add(url)
            pub = (d.get('pub_date', '') or '')[:10]
            out.append((url, pub))
    return out


def cache_present(url):
    p = os.path.join(CACHE_DIR, slug(url) + '.html')
    return os.path.exists(p) and os.path.getsize(p) > 5000


def cdx_inventory(url, retries=1, timeout=15):
    """Return (snapshot_count, status_codes_set) for a URL.

    Uses CDX with no statuscode filter so we see every archived response,
    not just successes. Lets us distinguish "no snapshots at all" from
    "snapshots exist but all are 403s".

    CDX is often slow / flaky. We use a short timeout and only one retry
    so an unresponsive CDX doesn't stall the whole audit. URLs that come
    back as CDX_UNREACHABLE can be re-probed on the next run.
    """
    cdx = (
        'https://web.archive.org/cdx/search/cdx?url='
        + urllib.parse.quote(url, safe='')
        + '&output=json&fl=timestamp,statuscode'
    )
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(cdx, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                rows = json.loads(r.read().decode())
            if not rows or rows[0] != ['timestamp', 'statuscode']:
                return (0, set())
            codes = set(row[1] for row in rows[1:])
            return (len(rows) - 1, codes)
        except Exception:
            if attempt < retries:
                time.sleep(3)
                continue
            return (-1, set())  # CDX itself unreachable


def classify(snap_count, codes):
    """Map (snapshot_count, status_codes) → classification label."""
    if snap_count == -1:
        return 'CDX_UNREACHABLE'
    if snap_count == 0:
        return 'WAITING'           # no snapshot exists — give Wayback time
    if '200' in codes:
        return 'HAS_200_RETRY'     # CDX shows a usable snapshot, scraper missed it
    if codes == {'403'} or codes <= {'403', '301', '302'}:
        return 'DATADOME_WALLED'   # only 403 (or redirects to 403) → needs manual save
    # Anything else (404 / 500 / mixed without 200 / etc.)
    return 'NO_USABLE_SNAPSHOT'


def main():
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('years', nargs='*',
                        help='Years to audit (e.g. 2024 2025). Default: 2014+.')
    parser.add_argument('--since', type=int,
                        help='Audit all years from this year onward.')
    parser.add_argument('--all-time', action='store_true',
                        help='Include pre-2014 (informational only — not in coverage scope).')
    parser.add_argument('--probe-cdx', action='store_true', default=True,
                        help='Probe CDX for each missing URL (default on).')
    parser.add_argument('--no-probe-cdx', dest='probe_cdx', action='store_false',
                        help='Skip CDX probing (faster, just lists missing URLs).')
    parser.add_argument('--cdx-pace', type=float, default=2.0,
                        help='Seconds between CDX requests (default 2).')
    args = parser.parse_args()

    years = None
    if args.years:
        years = set(args.years)
    elif args.since:
        years = set(str(y) for y in range(args.since, 2030))
    elif not args.all_time:
        # Default scope: 2014+ (when /pageoneplus/ corrections era began).
        years = set(str(y) for y in range(2014, 2030))

    print(f'Years scope: {sorted(years) if years else "all"}')
    urls = collect_urls(years)
    print(f'API-tagged correction URLs in scope: {len(urls):,}')

    cached, missing = [], []
    for u, p in urls:
        (cached if cache_present(u) else missing).append((u, p))
    print(f'  cached locally: {len(cached):,}')
    print(f'  missing:        {len(missing):,}')

    by_year_total = defaultdict(int)
    by_year_missing = defaultdict(int)
    for u, p in urls:
        y = p[:4] if p else 'unknown'
        by_year_total[y] += 1
    for u, p in missing:
        y = p[:4] if p else 'unknown'
        by_year_missing[y] += 1

    classifications = []  # list of dicts
    if args.probe_cdx and missing:
        print(f'\nProbing CDX for {len(missing)} missing URLs '
              f'(pace {args.cdx_pace}s, ~{int(len(missing) * args.cdx_pace / 60)} min)...')
        for i, (u, p) in enumerate(missing):
            time.sleep(args.cdx_pace)
            n, codes = cdx_inventory(u)
            label = classify(n, codes)
            classifications.append({
                'url': u,
                'page_date': p,
                'slug': slug(u),
                'snap_count': n,
                'status_codes': sorted(codes),
                'classification': label,
            })
            if (i + 1) % 25 == 0 or (i + 1) == len(missing):
                print(f'  [{i + 1}/{len(missing)}]')
    else:
        for u, p in missing:
            classifications.append({
                'url': u, 'page_date': p, 'slug': slug(u),
                'snap_count': None, 'status_codes': [],
                'classification': 'UNPROBED',
            })

    by_label = defaultdict(list)
    for c in classifications:
        by_label[c['classification']].append(c)
    by_label_year = defaultdict(lambda: defaultdict(int))
    for c in classifications:
        by_label_year[c['classification']][c['page_date'][:4]] += 1

    # ── Markdown report ────────────────────────────────────────────────────
    md = []
    md.append('# Corrections scraping gap report\n')
    md.append(f'Generated: {time.strftime("%Y-%m-%d %H:%M %Z")}\n')
    md.append(f'Scope: {", ".join(sorted(years)) if years else "all years"}\n')
    md.append('')
    md.append(f'- API-tagged correction URLs in scope: **{len(urls):,}**')
    md.append(f'- Cached locally: **{len(cached):,}**')
    md.append(f'- Missing: **{len(missing):,}**')
    if not missing:
        md.append('\n**No gaps. Coverage is complete for the scope.**\n')
    else:
        md.append('\n## Gaps by year\n')
        md.append('| Year | API total | Missing | Coverage |')
        md.append('|------|----------:|--------:|---------:|')
        for y in sorted(by_year_total):
            tot = by_year_total[y]
            miss = by_year_missing[y]
            pct = (tot - miss) / tot * 100 if tot else 0
            tag = '' if miss == 0 else (' ⚠️' if miss / tot > 0.02 else '')
            md.append(f'| {y} | {tot:,} | {miss:,} | {pct:.1f}%{tag} |')

        md.append('\n## Gaps by cause\n')
        labels_explained = [
            ('WAITING', 'Wayback hasn\'t crawled the URL yet. **Will likely backfill on its own** in weeks. Re-run the scraper periodically.'),
            ('HAS_200_RETRY', 'Wayback has a usable 200-status snapshot but our scraper somehow missed it. **Re-run the scraper** — should resolve.'),
            ('DATADOME_WALLED', 'Every Wayback snapshot is a DataDome 403. **Will not resolve on its own.** Needs your manual save (recipe below).'),
            ('NO_USABLE_SNAPSHOT', 'Wayback has snapshots but none are usable (e.g. 404 + 500 + redirects). Edge case — usually means the URL was never live; verify by hand.'),
            ('CDX_UNREACHABLE', 'CDX itself failed during the audit. Re-run the audit later.'),
            ('UNPROBED', 'Audit ran with --no-probe-cdx; classification skipped.'),
        ]
        for label, expl in labels_explained:
            entries = by_label[label]
            if not entries:
                continue
            md.append(f'### {label} — {len(entries):,} URLs')
            md.append(f'\n{expl}\n')
            yr_tally = sorted(by_label_year[label].items())
            md.append('By year: ' + ', '.join(f'{y}={n}' for y, n in yr_tally) + '\n')
            preview = entries[:30]
            for c in preview:
                md.append(f'- `{c["page_date"]}`  {c["url"]}')
            if len(entries) > 30:
                md.append(f'- … and {len(entries) - 30} more (see corrections_gap_report.json)')
            md.append('')

        if by_label['DATADOME_WALLED']:
            md.append('## Manual-save recipe for DATADOME_WALLED URLs\n')
            md.append('1. Open each URL above in a browser **while logged in to nytimes.com** (DataDome lets subscribers through).')
            md.append('2. Save Page As… → format **Webpage, HTML Only** (Cmd-S → uncheck "Page Source" if Safari).')
            md.append(f'3. Move/rename the saved file to `cache/corrections/<slug>.html` where `<slug>` is the 16-char hash from the JSON sidecar (or the filename `python -c \'import hashlib; print(hashlib.md5(b"URL").hexdigest()[:16])\'`).')
            md.append('4. Re-run the build pipeline: `python scripts/scrape_corrections.py YYYY` (will see the cache hit, skip the fetch) → `python scripts/build_corrections.py`.')
            md.append('')
            md.append('A small helper exists for step 3:')
            md.append('```')
            md.append('python scripts/import_correction_html.py <URL> path/to/saved.html')
            md.append('```')
            md.append('It computes the slug, copies the file into place, and prints a confirmation. (See `scripts/import_correction_html.py`.)\n')

    out_md = '\n'.join(md)
    with open(REPORT_MD, 'w') as fh:
        fh.write(out_md)
    print(f'\nWrote {REPORT_MD}')

    with open(REPORT_JSON, 'w') as fh:
        json.dump({
            'generated': time.strftime('%Y-%m-%dT%H:%M:%S%z'),
            'scope_years': sorted(years) if years else None,
            'totals': {
                'in_scope': len(urls),
                'cached': len(cached),
                'missing': len(missing),
            },
            'by_year_total': dict(by_year_total),
            'by_year_missing': dict(by_year_missing),
            'classifications': classifications,
        }, fh, indent=2)
    print(f'Wrote {REPORT_JSON}')

    # Print compact summary to stdout.
    print('\n=== summary ===')
    if not missing:
        print('✓ No gaps in scope.')
    else:
        for label, _ in labels_explained:
            n = len(by_label[label])
            if n:
                marker = '⚠ MANUAL' if label == 'DATADOME_WALLED' else ''
                print(f'  {label:<22s} {n:>5d}  {marker}')


if __name__ == '__main__':
    main()
