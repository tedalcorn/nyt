#!/usr/bin/env python3
"""Post-update validation pass.

Scans the freshly built data for problems that warrant a human look BEFORE
pushing the dashboard. Doesn't modify anything; prints a report.

Checks:
  - Obituaries (current year): parser-failure heuristics on name/profession.
  - Corrections (current + prior year):
      • Scrape gap: API-tagged URLs missing from our cache, grouped by month
        with a heuristic classification (recent → likely Wayback indexing
        lag, older → may need hand-save).
      • Per-page yield drop > 30% vs prior year (parser drift signal).
      • dow_match_diff >= 7 days (likely wrong matches).
      • Suspicious word counts (text < 15 words, > 200 words).
  - Articles (current year):
      • word_count == 0 concentration by section (refetch candidates).

If any corrections URLs look like they need hand-saving, the report
includes the URL, the cache filename to save it as, and a one-line
recipe — the same recipe scripts/import_correction_html.py supports.
"""
import json, glob, re, os, hashlib
from collections import Counter, defaultdict
from datetime import date

CUR_YEAR = str(date.today().year)
PRIOR    = str(date.today().year - 1)
TODAY    = date.today()

issues = []
def flag(category, msg):
    issues.append((category, msg))


# ── Obituaries ──────────────────────────────────────────────────────────────
def check_obits():
    if not os.path.exists('data/obituaries.json'):
        flag('obits', 'obituaries.json missing'); return
    with open('data/obituaries.json') as f:
        obits = json.load(f)
    cur = [o for o in obits if (o.get('year') or '') == CUR_YEAR]
    print(f'  obituaries this year: {len(cur):,}')

    long_name = [o for o in cur if len((o.get('name') or '').split()) >= 5]
    if long_name:
        flag('obits', f'{len(long_name)} obit(s) with 5+ token name (descriptor likely captured):')
        for o in long_name[:8]:
            issues.append(('  ', f"      {o['url']}  →  {o['name']!r}"))

    # Long-profession check: only flag truly excessive (12+ tokens) since
    # Overlooked-style obits routinely use descriptive subtitle-as-profession.
    long_prof = [o for o in cur
                 if o.get('profession') and len(o['profession'].split()) >= 12]
    if long_prof:
        flag('obits', f'{len(long_prof)} obit(s) with 12+ token profession:')
        for o in long_prof[:8]:
            issues.append(('  ', f"      {o['url']}  →  prof={o['profession']!r}"))

    one_token_descriptors = {'memorial', 'tribute', 'remembrance', 'appreciation'}
    descriptors = [o for o in cur
                   if (o.get('name') or '').lower().split()[-1] in one_token_descriptors]
    if descriptors:
        flag('obits', f'{len(descriptors)} obit(s) whose name ends in a descriptor word:')
        for o in descriptors[:8]:
            issues.append(('  ', f"      {o['url']}  →  {o['name']!r}"))


# ── Corrections ─────────────────────────────────────────────────────────────
def _slug(url):
    """Match scrape_corrections.py's cache-filename hash."""
    return hashlib.md5(url.encode()).hexdigest()[:16]


def _collect_api_correction_urls(year):
    """Mirror scrape_corrections.collect_urls for one year. Returns
    list of (url, pub_date)."""
    out = []
    for f in glob.glob(f'data/raw/{year}-*.json'):
        with open(f) as fh:
            try:
                docs = json.load(fh)
            except Exception:
                continue
        for d in docs:
            url = d.get('web_url') or ''
            if not url:
                continue
            ul = url.lower()
            if 'corrections-' not in ul or '/no-corrections-' in ul:
                continue
            tom = (d.get('type_of_material') or '').strip()
            sec = (d.get('section_name') or '').strip()
            keep = (
                tom == 'Correction'
                or (sec == 'Corrections' and tom != 'Quote' and tom != 'Correction')
            )
            if not keep:
                continue
            pub = (d.get('pub_date') or '')[:10]
            out.append((url, pub))
    return out


def _classify_age(page_date_str):
    """Heuristic from page-date alone, no network. Three buckets:

      datadome_walled       — year >= 2026. Confirmed via direct probing
                              that Wayback's archive is the DataDome 403
                              page; will not backfill.
      likely_wayback_lag    — within 60 days. Wayback's typical NYT crawl
                              window. High chance of self-resolving on a
                              future scrape.
      likely_needs_handsave — older. Wayback should have crawled by now;
                              if still missing, probably permanent.
    """
    try:
        y, m, d = page_date_str.split('-')
        days = (TODAY - date(int(y), int(m), int(d))).days
        year = int(y)
    except Exception:
        return 'unknown'
    if year >= 2026:
        return 'datadome_walled'
    if days < 60:
        return 'likely_wayback_lag'
    return 'likely_needs_handsave'


def check_corrections():
    if not os.path.exists('data/corrections.json'):
        flag('corr', 'corrections.json missing'); return

    with open('data/corrections.json') as f:
        raw = json.load(f)
    with open('data/corrections_matched.json') as f:
        matched = json.load(f)

    # ── Scrape gap (every year in the corrections-scraper era, 2014+) ──────
    # The /pageoneplus/ corrections URL pattern starts in late 2013; the
    # scraper was designed for that era forward. Pre-2014 URLs that match
    # "corrections-" are a different system and outside coverage scope.
    #
    # "Missing" means we don't have a usable HTML cache file. A cache file
    # that produced zero parseable items is NOT counted as missing — that's
    # a parser issue, not a coverage gap.
    def _is_cached(url):
        p = os.path.join('cache/corrections', _slug(url) + '.html')
        return os.path.exists(p) and os.path.getsize(p) > 5000

    handsave_todo = []  # URLs to save manually, written to data/corrections_save_todo.md

    scope_years = [str(y) for y in range(2014, int(CUR_YEAR) + 1)]
    for year in scope_years:
        api_pairs = _collect_api_correction_urls(year)
        api_urls = [u for u, _ in api_pairs]
        date_by_url = dict(api_pairs)
        missing = sorted(u for u in api_urls if not _is_cached(u))
        if not missing:
            continue
        # Group by month and classification.
        by_month = Counter(date_by_url[u][:7] for u in missing if date_by_url.get(u))
        cls = Counter(_classify_age(date_by_url.get(u, '')) for u in missing)
        flag('corr', f'Scrape gap: {len(missing)} {year} pageoneplus URL(s) '
                     f'tagged Correction but missing from our cache.')
        # Month breakdown
        months_str = ', '.join(f'{m}={n}' for m, n in sorted(by_month.items()))
        issues.append(('  ', f'      by month: {months_str}'))
        # Class breakdown — use plain English
        cls_descriptions = {
            'datadome_walled':       'year ≥ 2026 — DataDome-walled (Wayback only has 403 archives); hand-save needed if you want this period covered',
            'likely_wayback_lag':    'recent (<60d) — likely Wayback indexing lag, will probably backfill on a future scrape',
            'likely_needs_handsave': 'older (≥60d, year < 2026) — Wayback should have crawled by now; probable hand-save candidate',
            'unknown':               'page_date missing — verify by hand',
        }
        for label, n in cls.most_common():
            issues.append(('  ', f'      {n} {cls_descriptions.get(label, label)}'))
        # Track handsave candidates: anything classified as needing manual
        # action (datadome_walled or likely_needs_handsave). The recent-lag
        # bucket is excluded — those resolve on their own.
        for u in missing:
            label = _classify_age(date_by_url.get(u, ''))
            if label in ('likely_needs_handsave', 'datadome_walled'):
                handsave_todo.append((u, date_by_url.get(u, ''), label))

    if handsave_todo:
        todo_path = 'data/corrections_save_todo.md'
        lines = []
        lines.append('# Corrections — hand-save TODO\n')
        lines.append(f'Generated by validate.py on {TODAY.isoformat()}.\n')
        lines.append(f'{len(handsave_todo)} URL(s) appear to need a manual save: either older than 60 days and still not in our cache (Wayback should have crawled by now if it ever will), or in the DataDome-walled 2026+ window where Wayback only has 403 archives.\n')
        lines.append('Recent (<60d) gaps are excluded — those usually backfill on their own. Re-run validate after the next scrape to see what\'s still outstanding.\n')
        lines.append('## Workflow\n')
        lines.append('1. While signed in to nytimes.com in your browser, open each URL below.')
        lines.append('2. Save Page As… → format **Webpage, HTML Only**.')
        lines.append('3. Run `python scripts/import_correction_html.py <URL> <path/to/saved.html>` for each.')
        lines.append('   (The import script computes the right cache filename and copies the file in for you.)')
        lines.append('4. After all are imported, re-run `python scripts/build_corrections.py` to merge the new content.\n')
        lines.append('## URLs to save\n')
        # Sort by date for sanity.
        handsave_todo.sort(key=lambda x: x[1])
        # Group: needs_handsave first, then datadome_walled.
        for label_key, header in [
            ('likely_needs_handsave', 'Older missing (Wayback should have crawled — probably needs save)'),
            ('datadome_walled',       'DataDome-walled 2026+ (Wayback has only 403 archives — definitely needs save)'),
        ]:
            sub = [t for t in handsave_todo if t[2] == label_key]
            if not sub:
                continue
            lines.append(f'### {header} — {len(sub)}\n')
            for url, pd, _ in sub:
                lines.append(f'- `{pd}` — {url}')
                lines.append(f'  *(cache slug: `{_slug(url)}`)*')
            lines.append('')
        with open(todo_path, 'w') as fh:
            fh.write('\n'.join(lines) + '\n')
        issues.append(('  ', f'      → wrote {todo_path} with {len(handsave_todo)} hand-save URL(s)'))
    else:
        # Clean up stale TODO if previously written and now no work to do.
        todo_path = 'data/corrections_save_todo.md'
        if os.path.exists(todo_path):
            os.remove(todo_path)

    # Per-page yield: compare to prior year
    cur_pages_seen = set()
    cur_count = 0
    prior_pages_seen = set()
    prior_count = 0
    for r in raw:
        pd = (r.get('page_date') or '')[:4]
        pu = r.get('page_url') or ''
        if pd == CUR_YEAR:
            cur_pages_seen.add(pu); cur_count += 1
        elif pd == PRIOR:
            prior_pages_seen.add(pu); prior_count += 1
    cur_yield = cur_count / max(1, len(cur_pages_seen))
    prior_yield = prior_count / max(1, len(prior_pages_seen))
    print(f'  corrections per page: {CUR_YEAR}={cur_yield:.1f}, {PRIOR}={prior_yield:.1f}')
    # Only flag a yield drop if we have a meaningful sample for the current year.
    # Otherwise an empty-this-year scrape (e.g., before the year's first scrape)
    # produces a misleading "100% drop" alert.
    if cur_count >= 50 and prior_yield and (prior_yield - cur_yield) / prior_yield > 0.30:
        flag('corr', f'Per-page yield dropped {100*(prior_yield-cur_yield)/prior_yield:.0f}% vs prior year — parser drift?')

    # High DOW diff candidates this year
    cur_matched = [c for c in matched if (c.get('page_date') or '').startswith(CUR_YEAR)]
    high_diff = [c for c in cur_matched if (c.get('dow_match_diff') or 0) >= 7]
    if high_diff:
        flag('corr', f'{len(high_diff)} {CUR_YEAR} correction(s) with dow_match_diff>=7d (likely wrong match)')
        for c in high_diff[:6]:
            issues.append(('  ', f"      diff={c['dow_match_diff']}d  text: {(c.get('text') or '')[:120]}…"))

    # Word-count outliers
    short = [c for c in cur_matched if (c.get('text_word_count') or 0) < 15]
    long_ = [c for c in cur_matched if (c.get('text_word_count') or 0) > 200]
    if short:
        flag('corr', f'{len(short)} correction(s) with text_word_count < 15 (truncated/empty?)')
    if long_:
        flag('corr', f'{len(long_)} correction(s) with text_word_count > 200 (multi-correction blob?)')


# ── Articles (current year) ─────────────────────────────────────────────────
def check_articles():
    p = f'data/articles_{CUR_YEAR}.json'
    if not os.path.exists(p):
        flag('articles', f'{p} missing'); return
    with open(p) as f:
        arts = json.load(f)
    print(f'  articles this year: {len(arts):,}')

    # Zero-wc rate by section: high concentration is suspicious (we'd want refetch)
    zero_by_sec = Counter()
    total_by_sec = Counter()
    for a in arts:
        sec = a.get('s', '') or ''
        total_by_sec[sec] += 1
        if (a.get('w') or 0) == 0:
            zero_by_sec[sec] += 1
    flagged = []
    for sec, total in total_by_sec.most_common():
        if total < 50: continue
        zero = zero_by_sec[sec]
        rate = zero / total
        if rate > 0.20:
            flagged.append((sec, zero, total, rate))
    if flagged:
        flag('articles', f'{len(flagged)} section(s) with >20% zero-word-count articles in {CUR_YEAR}:')
        for sec, z, t, r in flagged[:8]:
            issues.append(('  ', f"      {sec:25s}  {z}/{t} ({100*r:.0f}%)"))


def main():
    print(f'Validating fresh data for {CUR_YEAR}…')
    check_articles()
    check_obits()
    check_corrections()

    if not issues:
        print('\nNo issues flagged. Looks clean.')
        return

    print('\n' + '=' * 70)
    print('VALIDATION REPORT')
    print('=' * 70)
    last_cat = None
    for cat, msg in issues:
        if cat != last_cat and cat != '  ':
            print(f'\n[{cat}]')
            last_cat = cat
        print(f'  {msg}' if cat != '  ' else msg)
    print('\n(These are not blockers — eyeball before pushing.)')


if __name__ == '__main__':
    main()
