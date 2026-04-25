"""Fetch corrections pages via Wayback Machine and parse correction items.

Live nytimes.com is behind DataDome which blocks headless scraping. Wayback
serves the same HTML without bot protection.

Each consolidated corrections page contains multiple correction blurbs that each
reference a previously-published article (often by date and headline). We pull
the page HTML, extract each correction's text, and try to recover the referenced
article's date + headline so we can later match it back to the article record.

Resumable via cache/corrections/<slug>.html (skip if cached).
"""
import json, os, re, sys, time, glob, hashlib, urllib.request, gzip
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

CACHE_DIR = 'cache/corrections'
OUT_PATH = 'data/corrections.json'
RAW_DIR = 'data/raw'

os.makedirs(CACHE_DIR, exist_ok=True)


def slug(url):
    return hashlib.md5(url.encode()).hexdigest()[:16]


def collect_urls(years):
    """Pull (url, pub_date, headline) for all type_of_material=Correction in given years."""
    out = []
    for f in sorted(glob.glob(os.path.join(RAW_DIR, '*.json'))):
        y = os.path.basename(f)[:4]
        if y not in years:
            continue
        with open(f) as fh:
            try:
                docs = json.load(fh)
            except Exception:
                continue
        for d in docs:
            if d.get('type_of_material') == 'Correction':
                url = d.get('web_url', '')
                pub = (d.get('pub_date', '') or '')[:10]
                head = (d.get('headline', {}) or {}).get('main', '') or ''
                if url:
                    out.append((url, pub, head))
    return out


def wayback_url(orig_url, target_date):
    """Build a direct Wayback URL — skip the availability API which is slow + flaky.
    The `id_/` flag returns the original (uninstrumented) HTML, and Wayback
    auto-redirects to the closest snapshot. Hint with target_date+30 days.
    """
    ts = target_date.replace('-', '') if target_date else '20250101'
    return f'https://web.archive.org/web/{ts}000000id_/{orig_url}'


def fetch_one(url, pub):
    """Sync fetch of one URL. Returns (status, url, html_or_None)."""
    path = os.path.join(CACHE_DIR, slug(url) + '.html')
    if os.path.exists(path) and os.path.getsize(path) > 5000:
        return ('cached', url, None)
    wb = wayback_url(url, pub)
    req = urllib.request.Request(wb, headers={'User-Agent': 'Mozilla/5.0 (compatible; CorrectionsResearch/1.0)'})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read()
            if r.headers.get('Content-Encoding') == 'gzip':
                raw = gzip.decompress(raw)
        html = raw.decode('utf-8', errors='replace')
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return ('no_snapshot', url, None)
        return (f'http{e.code}', url, None)
    except Exception as e:
        return (f'err:{type(e).__name__}', url, None)
    if len(html) < 5000:
        return ('too_short', url, None)
    with open(path, 'w') as fh:
        fh.write(html)
    return ('ok', url, html)


def fetch_all(urls, pace=3.5, max_retries=2):
    """Serial fetch with throttle. Wayback rate-limits aggressively if you go fast.
    pace = seconds between requests. ~4s/req → ~15/min, the published soft limit.
    """
    todo = []
    for u, p, h in urls:
        path = os.path.join(CACHE_DIR, slug(u) + '.html')
        if os.path.exists(path) and os.path.getsize(path) > 5000:
            continue
        todo.append((u, p))
    print(f'{len(urls)} total, {len(urls)-len(todo)} cached, {len(todo)} to fetch')
    if not todo:
        return
    counts = {}
    last = 0
    for i, (u, p) in enumerate(todo):
        # Throttle
        delta = time.time() - last
        if delta < pace:
            time.sleep(pace - delta)
        # Retry transient failures
        for attempt in range(max_retries + 1):
            last = time.time()
            status, _, _ = fetch_one(u, p)
            if status in ('ok', 'cached', 'no_snapshot', 'too_short'):
                break
            if attempt < max_retries:
                time.sleep(8 + 4 * attempt)
        counts[status] = counts.get(status, 0) + 1
        if (i + 1) % 10 == 0 or (i + 1) == len(todo):
            print(f'  [{i+1}/{len(todo)}] {counts}', flush=True)
    print(f'Final: {counts}')


# Lead-phrase regex tuned for NYT correction style.
RE_LEAD = re.compile(
    r'^(An?\s+(article|item|earlier\s+version|picture|caption|headline|review|chart|obituary|column|editorial|graphic|map|listing|profile|briefing|report|story|entry|feature|critic|crossword|recipe|news\s+article|news\s+analysis|essay|sub(?:headline|head)|table|video)|Because of|Due to|An\s+earlier|A\s+previous)',
    re.I,
)
RE_DATE_MONTH = re.compile(
    r'\b(January|February|March|April|May|June|July|August|September|October|November|December|Jan\.|Feb\.|Mar\.|Apr\.|Jun\.|Jul\.|Aug\.|Sept\.|Sep\.|Oct\.|Nov\.|Dec\.)\s+(\d{1,2})(?:,\s*(\d{4}))?',
    re.I,
)
RE_DAY_OF_WEEK = re.compile(r'\bon\s+(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday)\b', re.I)
RE_HEADLINE_QUOTED = re.compile(r'[\u201C"]([^\u201D"]{4,200})[\u201D"]')

MON_TO_N = {
    'jan': 1, 'january': 1, 'feb': 2, 'february': 2, 'mar': 3, 'march': 3,
    'apr': 4, 'april': 4, 'may': 5, 'jun': 6, 'june': 6, 'jul': 7, 'july': 7,
    'aug': 8, 'august': 8, 'sep': 9, 'sept': 9, 'september': 9,
    'oct': 10, 'october': 10, 'nov': 11, 'november': 11, 'dec': 12, 'december': 12,
}
DAY_TO_N = {'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3, 'friday': 4, 'saturday': 5, 'sunday': 6}


def parse_correction_page(html, page_url, page_pub_date):
    soup = BeautifulSoup(html, 'html.parser')
    body = soup.find('section', attrs={'name': 'articleBody'}) or soup.find('article')
    if not body:
        return []
    out = []
    for p in body.find_all('p'):
        text = p.get_text(' ', strip=True)
        if len(text) < 40 or len(text) > 2000:
            continue
        if not RE_LEAD.match(text):
            continue
        # Skip boilerplate footer items
        low = text.lower()
        if 'errors are corrected during the press run' in low:
            continue
        if 'to contact the newsroom' in low:
            continue
        if 'newspaper delivery questions' in low:
            continue
        if 'comments on opinion articles' in low:
            continue

        ref_date = None
        m = RE_DATE_MONTH.search(text)
        if m:
            mon = m.group(1).rstrip('.').lower()
            day = int(m.group(2))
            yr = int(m.group(3)) if m.group(3) else None
            mn = MON_TO_N.get(mon)
            if mn:
                if not yr and page_pub_date:
                    py = int(page_pub_date[:4])
                    pm = int(page_pub_date[5:7])
                    yr = py
                    if mn == 12 and pm == 1:
                        yr = py - 1
                if yr:
                    try:
                        ref_date = f'{yr:04d}-{mn:02d}-{day:02d}'
                    except Exception:
                        pass
        # Day-of-week fallback: derive date relative to page_pub_date
        if not ref_date and page_pub_date:
            dm = RE_DAY_OF_WEEK.search(text)
            if dm:
                try:
                    from datetime import date, timedelta
                    py, pmm, pdd = page_pub_date.split('-')
                    base = date(int(py), int(pmm), int(pdd))
                    target = DAY_TO_N[dm.group(1).lower()]
                    # Correction page mentions a recent past day
                    delta = (base.weekday() - target) % 7
                    if delta == 0:
                        delta = 7
                    ref_date = (base - timedelta(days=delta)).isoformat()
                except Exception:
                    pass

        ref_headline = None
        hm = RE_HEADLINE_QUOTED.search(text)
        if hm:
            ref_headline = hm.group(1).strip()

        out.append({
            'text': text,
            'ref_date': ref_date,
            'ref_headline': ref_headline,
            'page_url': page_url,
            'page_date': page_pub_date,
        })
    return out


def parse_all(urls):
    items = []
    no_html = 0
    no_corr = 0
    for url, pub, head in urls:
        path = os.path.join(CACHE_DIR, slug(url) + '.html')
        if not os.path.exists(path):
            no_html += 1
            continue
        with open(path) as fh:
            html = fh.read()
        if 'captcha-delivery.com' in html or len(html) < 2000:
            no_html += 1
            continue
        these = parse_correction_page(html, url, pub)
        if not these:
            no_corr += 1
            continue
        items.extend(these)
    print(f'Parsed {len(items)} corrections from {len(urls)} pages ({no_html} no html, {no_corr} no parse hits)')
    return items


def main():
    args = sys.argv[1:]
    if not args:
        print('Usage: scrape_corrections.py <year> [<year> ...]')
        sys.exit(1)
    years = set(args)
    urls = collect_urls(years)
    print(f'Years: {sorted(years)}, URLs: {len(urls)}')
    fetch_all(urls)
    items = parse_all(urls)
    with open(OUT_PATH, 'w') as fh:
        json.dump(items, fh, separators=(',', ':'))
    print(f'Saved {OUT_PATH} ({os.path.getsize(OUT_PATH):,} bytes)')


if __name__ == '__main__':
    main()
