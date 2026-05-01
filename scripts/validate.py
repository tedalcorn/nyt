#!/usr/bin/env python3
"""Post-update validation pass.

Scans the freshly built data for problems that warrant a human look BEFORE
pushing the dashboard. Doesn't modify anything; prints a report.

Checks (current year focus, since old data should be stable):
  - Obituaries:
      • Records with name longer than 4 tokens (likely descriptor parsed in)
      • Records with profession longer than 6 tokens (descriptor in profession)
      • Records with name == display_name == one of: Stapleton, Memorial, …
        (single-word descriptors — common parser failure mode)
  - Corrections:
      • Scrape gap: API tom=Correction pages missing from corrections.json
      • Per-page yield drop > 30% vs prior year
      • dow_match_diff >= 7 days (likely wrong matches)
      • Suspicious word counts (text < 15 words, > 200 words)
  - Articles:
      • word_count == 0 in current year non-stub article types
        (Magazine, Op-Ed, News etc.) — refetch candidates
"""
import json, glob, re, os
from collections import Counter
from datetime import date

CUR_YEAR = str(date.today().year)
PRIOR    = str(date.today().year - 1)

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
def check_corrections():
    if not os.path.exists('data/corrections.json'):
        flag('corr', 'corrections.json missing'); return

    with open('data/corrections.json') as f:
        raw = json.load(f)
    with open('data/corrections_matched.json') as f:
        matched = json.load(f)

    # Scrape gap: which API tom=Correction URLs aren't in our scraped pages?
    api_urls = set()
    for f in glob.glob(f'data/raw/{CUR_YEAR}-*.json'):
        with open(f) as fh:
            try: docs = json.load(fh)
            except: continue
        for d in docs:
            if d.get('type_of_material') == 'Correction':
                api_urls.add(d.get('web_url') or '')
    scraped_urls = set((r.get('page_url') or '') for r in raw
                       if (r.get('page_url') or '').startswith((f'https://www.nytimes.com/{CUR_YEAR}/',
                                                                 f'http://www.nytimes.com/{CUR_YEAR}/')))
    missing = api_urls - scraped_urls
    if missing:
        flag('corr', f'Scrape gap: {len(missing)} {CUR_YEAR} pageoneplus URL(s) tagged Correction in API but not in our scrape')
        for u in sorted(missing)[:6]:
            issues.append(('  ', f"      {u}"))

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
