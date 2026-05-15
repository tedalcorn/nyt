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
from datetime import date, timedelta
from bs4 import BeautifulSoup

CACHE_DIR = 'cache/corrections'
OUT_PATH = 'data/corrections.json'
RAW_DIR = 'data/raw'

os.makedirs(CACHE_DIR, exist_ok=True)


def slug(url):
    return hashlib.md5(url.encode()).hexdigest()[:16]


def collect_urls(years):
    """Pull (url, pub_date, headline) for daily corrections pages in given years.

    Two tagging conventions cover the SAME daily-page URL pattern but with no
    overlap, so we have to union them:
      - tom=Correction (primary tagging, ~94% of pages)
      - tom=News + section=Corrections (residual, ~6%)
    Quote of the Day items live in section=Corrections too; filter them out.
    Empty `/no-corrections-DATE/` placeholder URLs are also skipped.
    """
    out = []
    seen = set()
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
            url = d.get('web_url', '') or ''
            if not url:
                continue
            tom = (d.get('type_of_material') or '').strip()
            sec = (d.get('section_name') or '').strip()
            ul = url.lower()
            # Restrict to the daily-corrections URL slug; skip empty-day stubs.
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
            head = (d.get('headline', {}) or {}).get('main', '') or ''
            out.append((url, pub, head))
    return out


def cdx_find_ts(url, pub):
    """Use CDX API to find the nearest 200-status snapshot timestamp.

    Returns a timestamp string like '20160124183045', or None if not found.
    Useful for pages where the naive pub_date hint misses Wayback's index.
    CDX is rate-limited too — call this only when id_/ returns 404 or 403.
    """
    import urllib.parse as _up
    cdx = (
        'https://web.archive.org/cdx/search/cdx?url=' + _up.quote(url, safe='')
        + '&output=json&limit=1&fl=timestamp&filter=statuscode:200'
        + '&closest=' + (pub.replace('-', '') if pub else '20200101')
    )
    try:
        req = urllib.request.Request(cdx, headers={'User-Agent': 'Mozilla/5.0 (compatible; CorrectionsResearch/1.0)'})
        with urllib.request.urlopen(req, timeout=20) as r:
            rows = json.loads(r.read().decode())
        # rows[0] is the header ['timestamp']; rows[1] is the first result
        if len(rows) > 1:
            return rows[1][0]
    except Exception:
        pass
    return None


def _read_html(raw, resp_headers):
    """Decompress and decode raw bytes from Wayback, handling implicit gzip."""
    enc = (resp_headers.get('Content-Encoding') or '').lower()
    if enc == 'gzip' or raw[:2] == b'\x1f\x8b':
        try:
            raw = gzip.decompress(raw)
        except Exception:
            pass
    return raw.decode('utf-8', errors='replace')


def fetch_one(url, pub):
    """Sync fetch of one URL from Wayback. Returns (status, url, html_or_None).

    Strategy (in order):
      1. id_/ URL with pub_date hint → preserves original HTML, no toolbar.
      2. If 404 OR 403: CDX lookup filtered to statuscode:200 → finds any
         "real content" snapshot, skipping DataDome-blocked archives. Then
         retry id_/ at that timestamp.
      3. If still 403 (only DataDome-blocked archives exist): terminal —
         classify as `archived_403_only` so the outer retry loop doesn't
         waste cycles on it. The cache is intentionally not written, so a
         future run can re-check Wayback once a non-blocked snapshot exists.

    NYT enabled DataDome on /pageoneplus/ pages somewhere between mid-2025
    and Jan 2026. For URLs in that window, Wayback's crawler gets the same
    403 a casual scraper would, and faithfully archives that 403 page —
    so id_/ returns 403 even when CDX shows snapshots exist.
    """
    path = os.path.join(CACHE_DIR, slug(url) + '.html')
    if os.path.exists(path) and os.path.getsize(path) > 5000:
        return ('cached', url, None)

    ts = pub.replace('-', '') if pub else '20200101'
    ua = {'User-Agent': 'Mozilla/5.0 (compatible; CorrectionsResearch/1.0)'}

    def _try_wb(wb_url):
        req = urllib.request.Request(wb_url, headers=ua)
        with urllib.request.urlopen(req, timeout=35) as r:
            raw = r.read()
            return _read_html(raw, r.headers)

    # Attempt 1: standard id_/ URL with pub_date hint.
    wb1 = f'https://web.archive.org/web/{ts}000000id_/{url}'
    try:
        html = _try_wb(wb1)
        if len(html) >= 5000:
            with open(path, 'w') as fh:
                fh.write(html)
            return ('ok', url, html)
        return ('too_short', url, None)
    except urllib.error.HTTPError as e:
        first_code = e.code
    except Exception:
        first_code = 0

    # Attempt 2: if 404 / 403 / network-level failure, ask CDX for ANY
    # statuscode:200 snapshot. Filtering to 200 automatically skips
    # DataDome-blocked archives that return 403 at the origin and were
    # faithfully recorded as 403 by Wayback. first_code == 0 means a
    # connection/socket error on attempt 1 — also worth a CDX shot before
    # giving up.
    if first_code in (0, 403, 404):
        time.sleep(2)
        real_ts = cdx_find_ts(url, pub)
        if not real_ts:
            # No 200-status snapshot exists. Could be:
            #   - Wayback hasn't crawled the URL yet (recent, will retry next run)
            #   - Every snapshot is a DataDome 403 (will not improve without
            #     Wayback re-crawling from a different vantage point)
            # Either way: terminal for this run. Don't cache so we re-check later.
            return ('no_good_snapshot', url, None)
        time.sleep(2)
        wb2 = f'https://web.archive.org/web/{real_ts}id_/{url}'
        try:
            html = _try_wb(wb2)
            if len(html) >= 5000:
                with open(path, 'w') as fh:
                    fh.write(html)
                return ('ok_cdx', url, html)
            return ('too_short', url, None)
        except urllib.error.HTTPError as e:
            # CDX promised a 200 but the snapshot fetch fails. Rare —
            # treat as no_good_snapshot so we re-check next run.
            if e.code in (403, 404):
                return ('no_good_snapshot', url, None)
            return (f'http{e.code}', url, None)
        except Exception as e:
            return (f'err:{type(e).__name__}', url, None)

    return (f'http{first_code}', url, None)


def fetch_all(urls, pace=6.0, max_retries=3):
    """Serial fetch with throttle. Pace is conservative by default.

    Wayback's soft limit: ~15 req/min. 6s/request is safely under that.
    Exponential backoff on 429 / 503 / connection errors.
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
        # Retry on transient rate-limit errors with exponential backoff
        for attempt in range(max_retries + 1):
            last = time.time()
            status, _, _ = fetch_one(u, p)
            # Retry only on transient rate-limit / availability codes
            # (429 / 503). Everything else is terminal — including
            # connection errors (err:*) and other HTTP errors that
            # already went through the CDX fallback in fetch_one.
            terminal = status not in ('http429', 'http503')
            if terminal:
                break
            # Rate-limited or connection error — back off
            wait = 20 * (2 ** attempt)
            if attempt < max_retries:
                print(f'    [{attempt+1}/{max_retries}] {status} — backing off {wait}s', flush=True)
                time.sleep(wait)
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
RE_LAST_DAY = re.compile(r'\blast\s+(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday)\b', re.I)
# "this weekend" / "last weekend" — both mean the most recent Sunday relative
# to the correction page's pub_date. Sunday-section corrections (Magazine, Book
# Review, T Magazine) almost always use this phrasing instead of a date or weekday.
RE_WEEKEND = re.compile(r'\b(this|last)\s+weekend\b', re.I)
# Daily-corrections URL slug carries the page date when text-based parsing fails:
# /pageoneplus/corrections-MONTH-D-YYYY.html. The slug date is the *correction*
# pub date (page_date), so when we fall back here we record it as a hint and
# let the matcher search the broader window.
RE_URL_DATE = re.compile(r'/corrections-([a-z]+)-(\d{1,2})-(\d{4})', re.I)
RE_HEADLINE_QUOTED = re.compile(r'[\u201C"]([^\u201D"]{4,200})[\u201D"]')
# NYT article URL inside a corrections paragraph — points at the corrected piece.
RE_NYT_ARTICLE_URL = re.compile(r'^https?://(?:www\.)?nytimes\.com(/\d{4}/\d{2}/\d{2}/[^?#\s]+\.html)', re.I)
# Skip URLs that point at the corrections page itself, related corrections, or
# tangential refs (interactives, topic pages).
RE_SKIP_URL_PATH = re.compile(r'/pageoneplus/', re.I)

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

        # Inline reference: first <a href> in the paragraph that points at an
        # NYT article (not another corrections page). NYT correction text often
        # links the word "article"/"obituary"/"caption" to the original piece —
        # this is the most reliable matching signal we have.
        inline_url = None
        for a in p.find_all('a'):
            href = (a.get('href') or '').strip()
            m = RE_NYT_ARTICLE_URL.match(href)
            if not m:
                continue
            path = m.group(1)
            if RE_SKIP_URL_PATH.search(path):
                continue
            inline_url = path
            break
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
        # Day-of-week fallback: derive date relative to page_pub_date.
        # Tries in priority order:
        #   1. "on Sunday" / "on Monday" → most recent matching weekday before page_date
        #   2. "last Sunday" / "last Monday" → same logic; Times uses "last" interchangeably
        #   3. "this weekend" / "last weekend" → most recent Sunday before page_date
        if not ref_date and page_pub_date:
            dm = RE_DAY_OF_WEEK.search(text) or RE_LAST_DAY.search(text)
            target_day = None
            if dm:
                target_day = DAY_TO_N[dm.group(1).lower()]
            elif RE_WEEKEND.search(text):
                target_day = DAY_TO_N['sunday']
            if target_day is not None:
                try:
                    py, pmm, pdd = page_pub_date.split('-')
                    base = date(int(py), int(pmm), int(pdd))
                    delta = (base.weekday() - target_day) % 7
                    if delta == 0:
                        delta = 7
                    ref_date = (base - timedelta(days=delta)).isoformat()
                except Exception:
                    pass
        # Last-resort: just stamp the page's own date so the matcher has a window.
        # The matcher widens ±3 days around ref_date, which catches articles from
        # the prior week. Better than skipping entirely.
        if not ref_date and page_pub_date:
            ref_date = page_pub_date

        ref_headline = None
        hm = RE_HEADLINE_QUOTED.search(text)
        if hm:
            ref_headline = hm.group(1).strip()

        out.append({
            'text': text,
            'ref_date': ref_date,
            'ref_headline': ref_headline,
            'inline_url': inline_url,
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
        with open(path, encoding='utf-8', errors='replace') as fh:
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

    # SAFE MERGE: load existing data and merge rather than overwrite.
    # This prevents running a single-year scrape from destroying all other years' data.
    existing = []
    if os.path.exists(OUT_PATH):
        try:
            with open(OUT_PATH) as fh:
                existing = json.load(fh)
        except (json.JSONDecodeError, ValueError):
            existing = []

    # Remove existing entries for the years being re-scraped, then add new ones
    keep = [c for c in existing if c.get('page_date', '')[:4] not in years]
    merged = keep + items
    merged.sort(key=lambda c: c.get('page_date', ''))

    with open(OUT_PATH, 'w') as fh:
        json.dump(merged, fh, separators=(',', ':'))
    print(f'Saved {OUT_PATH}: {len(merged):,} total ({len(items):,} new, {len(keep):,} retained)')


if __name__ == '__main__':
    main()
