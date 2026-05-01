"""Re-fetch word counts for articles the NYT API reported as 0 words.

Many of these are tagged "Interactive Feature" but have substantial body text
(e.g. Magazine interviews, Books features, policy explainers). We pull the
Wayback snapshot, run it through trafilatura, count words, and save a
URL → measured_wc mapping that build_data.py can merge in.

Resumable via cache/refetch/<slug>.html.
"""
import json, os, re, sys, glob, hashlib, urllib.request, gzip
from concurrent.futures import ThreadPoolExecutor, as_completed
import trafilatura

CACHE_DIR = 'cache/refetch'
OUT_PATH = 'data/measured_wc.json'
RAW_DIR = 'data/raw'

os.makedirs(CACHE_DIR, exist_ok=True)

# Type tags we *do* want to measure (these may have real text despite zero-wc).
INCLUDE_TOM = {'Interactive Feature', 'News', 'Live Blog Post', 'Op-Ed', '', None}
EXCLUDE_TOM = {'Correction', 'Slideshow', 'Video', 'Audio', 'Briefing'}


def slug(url):
    return hashlib.md5(url.encode()).hexdigest()[:16]


def collect_zero_wc_urls(years):
    """Pull (url, pub_date, section, type, headline) for zero-wc articles."""
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
            if d.get('word_count', 0) != 0:
                continue
            tom = d.get('type_of_material', '') or ''
            if tom in EXCLUDE_TOM:
                continue
            url = d.get('web_url', '')
            if not url:
                continue
            pub = (d.get('pub_date', '') or '')[:10]
            sec = d.get('section_name', '') or ''
            head = (d.get('headline', {}) or {}).get('main', '') or ''
            out.append((url, pub, sec, tom, head))
    return out


def wayback_url(orig_url, target_date):
    ts = target_date.replace('-', '') if target_date else '20250101'
    return f'https://web.archive.org/web/{ts}000000id_/{orig_url}'


def fetch_one(url, pub):
    path = os.path.join(CACHE_DIR, slug(url) + '.html')
    if os.path.exists(path) and os.path.getsize(path) > 3000:
        return ('cached', url)
    wb = wayback_url(url, pub)
    req = urllib.request.Request(wb, headers={'User-Agent': 'Mozilla/5.0 (compatible; WCRefetch/1.0)'})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read()
            if r.headers.get('Content-Encoding') == 'gzip':
                raw = gzip.decompress(raw)
        html = raw.decode('utf-8', errors='replace')
    except Exception as e:
        return (f'err:{e}', url)
    if len(html) < 3000:
        return ('too_short', url)
    with open(path, 'w') as fh:
        fh.write(html)
    return ('ok', url)


def fetch_all(rows, workers=6):
    todo = []
    for url, pub, *_ in rows:
        path = os.path.join(CACHE_DIR, slug(url) + '.html')
        if os.path.exists(path) and os.path.getsize(path) > 3000:
            continue
        todo.append((url, pub))
    print(f'{len(rows)} candidates, {len(rows)-len(todo)} cached, {len(todo)} to fetch')
    if not todo:
        return
    counts = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_one, u, p): u for (u, p) in todo}
        for i, fut in enumerate(as_completed(futs)):
            status, _ = fut.result()
            key = 'err' if status.startswith('err') else status
            counts[key] = counts.get(key, 0) + 1
            if (i + 1) % 50 == 0 or (i + 1) == len(todo):
                print(f'  [{i+1}/{len(todo)}] {counts}')
    print(f'Final fetch: {counts}')


def measure_one(url):
    path = os.path.join(CACHE_DIR, slug(url) + '.html')
    if not os.path.exists(path):
        return None
    with open(path) as fh:
        html = fh.read()
    if len(html) < 3000 or 'captcha-delivery.com' in html:
        return None
    text = trafilatura.extract(html, include_comments=False, include_tables=False, no_fallback=False)
    if not text:
        return 0
    return len(text.split())


def measure_all(rows):
    out = {}
    by_section_total = {}
    by_section_wc = {}
    none_count = 0
    for url, pub, sec, tom, head in rows:
        wc = measure_one(url)
        if wc is None:
            none_count += 1
            continue
        out[url] = wc
        by_section_total.setdefault(sec, []).append(wc)
        by_section_wc[sec] = by_section_wc.get(sec, 0) + wc
    print(f'\nMeasured {len(out)} URLs ({none_count} no html / unparseable)')
    if out:
        wcs = list(out.values())
        wcs.sort()
        med = wcs[len(wcs)//2]
        avg = sum(wcs)/len(wcs)
        print(f'  word counts: min={min(wcs)}, median={med}, avg={avg:.0f}, max={max(wcs)}')
        n_substantial = sum(1 for w in wcs if w >= 200)
        print(f'  substantial (>=200 words): {n_substantial} ({100*n_substantial/len(wcs):.0f}%)')
        print('\n  Top sections by recovered wc count:')
        for s, vs in sorted(by_section_total.items(), key=lambda kv: -len(kv[1]))[:10]:
            n_sub = sum(1 for w in vs if w >= 200)
            print(f'    {s:25s} | n={len(vs):4d} | substantial={n_sub:4d} | median={sorted(vs)[len(vs)//2]}')
    return out


def main():
    args = sys.argv[1:]
    if not args:
        print('Usage: refetch_wordcounts.py <year> [<year> ...]')
        sys.exit(1)
    years = set(args)
    rows = collect_zero_wc_urls(years)
    print(f'Years: {sorted(years)}, candidates: {len(rows)}')
    fetch_all(rows)
    out = measure_all(rows)
    if os.path.exists(OUT_PATH):
        with open(OUT_PATH) as fh:
            existing = json.load(fh)
        existing.update(out)
        out = existing
    with open(OUT_PATH, 'w') as fh:
        json.dump(out, fh, separators=(',', ':'), sort_keys=True)
    print(f'Saved {OUT_PATH} ({os.path.getsize(OUT_PATH):,} bytes, {len(out)} entries)')


if __name__ == '__main__':
    main()
