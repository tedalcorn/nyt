"""Surgical augmenter for data/corrections_matched.json.

Adds three fields per correction:
  text_word_count    — len(text.split()) for outlier spotting
  dow_inferred_date  — print-date implied by day-of-week mention in the
                       correction text. "last <DAY>" subtracts a week.
  dow_match_diff     — |dow_inferred_date - match_url's digital pub date|
                       in days, when both are present. Big values flag
                       likely wrong matches or unusually large print vs
                       digital publication lag.

Idempotent. Doesn't touch corrections.json (the raw scrape).
"""
import json, re
from datetime import date, timedelta

PATH = 'data/corrections_matched.json'

DOW_NAMES = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
DOW_TO_NUM = {n: i for i, n in enumerate(DOW_NAMES)}
DOW_RE = re.compile(r'\b(last|on)?\s*(' + '|'.join(DOW_NAMES) + r')\b', re.I)
URL_DATE = re.compile(r'^/(\d{4})/(\d{2})/(\d{2})/')


def infer_print_date(text, page_date_str):
    """Return (iso_date | None, dow_name | None)."""
    if not text or not page_date_str:
        return None, None
    m = DOW_RE.search(text)
    if not m:
        return None, None
    qualifier = (m.group(1) or '').lower()
    dow_name = m.group(2).capitalize()
    target = DOW_TO_NUM[dow_name]
    pd = date.fromisoformat(page_date_str)
    most_recent = None
    for delta in range(0, 8):
        c = pd - timedelta(days=delta)
        if c.weekday() == target:
            most_recent = c
            break
    if most_recent is None:
        return None, dow_name
    if qualifier == 'last':
        most_recent -= timedelta(days=7)
    return most_recent.isoformat(), dow_name


def match_url_date(match_url):
    if not match_url:
        return None
    m = URL_DATE.match(match_url)
    if not m:
        return None
    return f'{m.group(1)}-{m.group(2)}-{m.group(3)}'


def main():
    with open(PATH) as f:
        rows = json.load(f)
    print(f'Loaded {len(rows):,} rows from {PATH}')

    n_dow = 0
    n_dow_match = 0
    n_diff_zero = 0
    n_diff_le1 = 0
    n_diff_ge7 = 0
    big_diffs = []  # samples for report

    for r in rows:
        text = r.get('text') or ''
        page_date = r.get('page_date') or ''
        # 1. word count
        r['text_word_count'] = len(text.split())
        # 2. DOW-inferred print date
        inferred, dow_name = infer_print_date(text, page_date)
        r['dow_inferred_date'] = inferred
        r['dow_name'] = dow_name
        # 3. distance from matched article's digital pub date
        diff = None
        mu_date = match_url_date(r.get('match_url'))
        if inferred and mu_date:
            d_inf = date.fromisoformat(inferred)
            d_mu = date.fromisoformat(mu_date)
            diff = abs((d_inf - d_mu).days)
        r['dow_match_diff'] = diff
        if dow_name:
            n_dow += 1
        if diff is not None:
            n_dow_match += 1
            if diff == 0: n_diff_zero += 1
            if diff <= 1: n_diff_le1 += 1
            if diff >= 7:
                n_diff_ge7 += 1
                if len(big_diffs) < 8:
                    big_diffs.append(r)

    with open(PATH, 'w') as f:
        json.dump(rows, f, separators=(',', ':'))
    print(f'Wrote {PATH}')

    # Report
    print()
    print(f'rows with DOW parsed: {n_dow:,} ({100*n_dow/len(rows):.1f}%)')
    print(f'rows with DOW + match_url: {n_dow_match:,}')
    if n_dow_match:
        print(f'  diff == 0 days:  {n_diff_zero:,} ({100*n_diff_zero/n_dow_match:.1f}%)')
        print(f'  diff <= 1 days:  {n_diff_le1:,} ({100*n_diff_le1/n_dow_match:.1f}%)')
        print(f'  diff >= 7 days:  {n_diff_ge7:,} ({100*n_diff_ge7/n_dow_match:.1f}%)  <-- review candidates')

    wcs = sorted(r['text_word_count'] for r in rows)
    n = len(wcs)
    p = lambda q: wcs[min(n-1, int(q*n))]
    print()
    print(f'text_word_count: min={wcs[0]}, p10={p(0.1)}, median={wcs[n//2]}, p90={p(0.9)}, p99={p(0.99)}, max={wcs[-1]}')
    print(f'  outliers <15w: {sum(1 for w in wcs if w<15)} (likely truncated/empty)')
    print(f'  outliers >120w: {sum(1 for w in wcs if w>120)} (likely multi-correction blocks)')

    print()
    print('Big-diff samples (review candidates):')
    for r in big_diffs:
        print(f"  diff={r['dow_match_diff']}d  dow={r['dow_name']} inferred={r['dow_inferred_date']} match={r['match_url']}")
        print(f"    text: {(r.get('text') or '')[:160]}...")
        print()


if __name__ == '__main__':
    main()
