"""Match parsed corrections to article records.

`scrape_corrections.py` writes `data/corrections.json` containing per-correction
items. We try to match each one back to a specific article in articles_YYYY.json.

Corrections rarely quote the article headline. They describe by topic:
"An article on Thursday about [Nosferatu]". So we extract content keywords
from the correction body and score articles whose headline + subjects + URL
slug overlap with those keywords, within ±3 days of the referenced date.

Outputs:
  data/corrections_matched.json   — full per-correction records w/ matched url
  data/corrections_summary.json   — counts/aggregates for the dashboard
"""
import json, os, re, glob
from datetime import date, timedelta
from collections import Counter, defaultdict

CORR_PATH = 'data/corrections.json'
OUT_MATCHED = 'data/corrections_matched.json'
OUT_SUMMARY = 'data/corrections_summary.json'


def load_articles_by_date():
    """Return ({YYYY-MM-DD: [...]}, {url: art}) from condensed yearly files.
    Each article has all the fields we need plus a precomputed token set.
    The url→article index lets us short-circuit when a correction's inline_url
    is set (corrections that link to the original piece in their HTML)."""
    by_date = defaultdict(list)
    by_url = {}
    for f in sorted(glob.glob('data/articles_*.json')):
        with open(f) as fh:
            arts = json.load(fh)
        for a in arts:
            d = a.get('d', '')[:10]
            if not d:
                continue
            # Build token bag from headline + subjects + URL slug
            slug = a.get('u', '').replace('-', ' ').replace('/', ' ')
            text = ' '.join([a.get('h', '') or '', slug, ' '.join(a.get('sb', []) or [])])
            tokens = set(w.lower() for w in WORD.findall(text) if len(w) > 3)
            rec = {
                'u': a.get('u', ''),
                'h': a.get('h', '') or '',
                'a': a.get('a', []) or [],
                'sec': a.get('s', '') or '',
                'tokens': tokens,
            }
            by_date[d].append(rec)
            if rec['u']:
                by_url[rec['u']] = rec
    return by_date, by_url


WORD = re.compile(r'\w+')

# Lightweight subcategory tags. We track these silently in the data so we can
# decide later whether to surface them as filters in the UI.
TAG_PATTERNS = [
    ('photo_caption',   re.compile(r'\b(picture|photo)(graph)?\b[^.]*\bcaption\b|\bcaption\b[^.]*\b(picture|photo)', re.I)),
    ('editing_error',   re.compile(r'\bediting error\b|\bbecause of an editing\b', re.I)),
    ('production_error',re.compile(r'\bproduction error\b', re.I)),
]


def correction_tags(text):
    if not text: return []
    return [name for name, pat in TAG_PATTERNS if pat.search(text)]

# Common words to drop — they're in nearly every correction.
STOP = set("""
about article above abstract above an and any are article articles because been
both came caption could critic critics cxn date dear earlier editing edition
editorial editor effect entry essay error errors first from full graphic group
have here however identifier item items just last left less likely listing made
many maps misattributed misidentified misnamed misquoted mistake misstated more
much name names note notes obit obituaries obituary online opinion order page
paragraph paragraphs picture pictures preceded present press print printed prior
production reading reads recipe ref referenced referred report reports review
same says second show shown some still story subhead subheading subheadline
table that they this through time times today translated upper version versions
when which while will with word words wrong writer note article also was
actually appearance been after thursday wednesday tuesday monday friday saturday
sunday january february march april june july august september october november
december referred describing described regarding which whose
""".split())


def extract_topic_tokens(text):
    """Pull keyword-ish tokens from correction text — drop stop-words."""
    toks = set()
    for w in WORD.findall(text or ''):
        wl = w.lower()
        if len(w) <= 3 or wl in STOP:
            continue
        toks.add(wl)
    return toks


def best_match(items, topic_tokens, ref_headline):
    """Return (article, score, n_matched).

    Score = matches / min(article_tokens, 8) — rewards specific-overlap. We also
    return raw match count so caller can require a minimum.
    """
    if not items:
        return None, 0.0, 0
    if not topic_tokens and not ref_headline:
        return None, 0.0, 0
    best = None
    best_s = 0.0
    best_n = 0
    rh_l = (ref_headline or '').lower()
    for art in items:
        # Containment: ref headline is substring of article headline → strong signal
        if rh_l and len(rh_l) >= 6 and rh_l in art['h'].lower():
            return art, 1.0, 99
        if not topic_tokens or not art['tokens']:
            continue
        match = topic_tokens & art['tokens']
        if not match:
            continue
        s = len(match) / min(max(len(art['tokens']), 4), 8)
        if s > best_s:
            best_s = s
            best = art
            best_n = len(match)
    return best, best_s, best_n


def main():
    if not os.path.exists(CORR_PATH):
        print(f'No {CORR_PATH} — run scrape_corrections.py first')
        return
    with open(CORR_PATH) as fh:
        corrections = json.load(fh)
    print(f'Loaded {len(corrections)} parsed corrections')

    by_date, by_url = load_articles_by_date()
    print(f'Indexed {sum(len(v) for v in by_date.values())} articles across {len(by_date)} dates ({len(by_url)} unique URLs)')

    matched = []
    no_ref = 0
    no_match = 0
    matched_n = 0
    inline_n = 0
    by_section = Counter()
    by_author = Counter()

    def _emit_match(c, art, score, source):
        """Append a matched record. Pulls section/headline/authors from art."""
        sec = art['sec']
        authors = art['a'] or []
        by_section[sec] += 1
        for au in authors:
            by_author[au] += 1
        matched.append({
            **c,
            'match_url': art['u'],
            'match_score': round(score, 3),
            'match_section': sec,
            'match_headline': art['h'],
            'match_authors': authors,
            'match_source': source,
            'tags': correction_tags(c.get('text')),
        })

    def _emit_unmatched(c, score):
        matched.append({
            **c,
            'match_url': None,
            'match_score': round(score, 3),
            'match_section': None,
            'match_headline': None,
            'match_authors': None,
            'match_source': None,
            'tags': correction_tags(c.get('text')),
        })

    for c in corrections:
        # Strongest signal: an inline <a> in the correction HTML pointed at the
        # original article. Trust it without further token scoring.
        inline = c.get('inline_url')
        if inline and inline in by_url:
            inline_n += 1
            matched_n += 1
            _emit_match(c, by_url[inline], 1.0, 'inline')
            continue

        ref_date = c.get('ref_date')
        ref_head = c.get('ref_headline')
        if not ref_date:
            no_ref += 1
            _emit_unmatched(c, 0)
            continue
        try:
            y, mm, dd = ref_date.split('-')
            base = date(int(y), int(mm), int(dd))
        except Exception:
            no_ref += 1
            _emit_unmatched(c, 0)
            continue
        pool = []
        for offset in (-3, -2, -1, 0, 1, 2, 3):
            d2 = (base + timedelta(days=offset)).isoformat()
            pool.extend(by_date.get(d2, []))
        topic_toks = extract_topic_tokens(c.get('text', ''))
        m, score, n_match = best_match(pool, topic_toks, ref_head)
        # Accept if substring containment (score=1.0) or ≥2 distinct topic tokens overlap
        # within the date window.
        if m and (n_match >= 2 or score == 1.0):
            matched_n += 1
            _emit_match(c, m, score, 'tokens')
        else:
            no_match += 1
            _emit_unmatched(c, score)

    print(f'Matched: {matched_n}/{len(corrections)} ({100*matched_n/len(corrections):.0f}%)')
    print(f'  via inline URL: {inline_n}')
    print(f'  via tokens:     {matched_n - inline_n}')
    print(f'  no ref_date: {no_ref}')
    print(f'  no match (ref present): {no_match}')

    # Duplicate detection: corrections that resolve to the same article (and share
    # the same set of authors) get rolled up. The earliest by page_date is kept
    # as the primary; later page URLs are listed as `additional_pages` so the UI
    # can render them as "(Additional notification)" links.
    by_key = defaultdict(list)
    for i, c in enumerate(matched):
        if not c.get('match_url'):
            continue
        key = (c['match_url'], tuple(sorted(c.get('match_authors') or [])))
        by_key[key].append(i)
    n_dupes = 0
    drop = set()
    for key, idxs in by_key.items():
        if len(idxs) < 2:
            continue
        # Sort by page_date asc (then index for stability)
        idxs_sorted = sorted(idxs, key=lambda i: (matched[i].get('page_date') or '', i))
        primary = idxs_sorted[0]
        extras = idxs_sorted[1:]
        addl = []
        for j in extras:
            addl.append({'page_url': matched[j].get('page_url'), 'page_date': matched[j].get('page_date')})
            drop.add(j)
        matched[primary]['additional_pages'] = addl
        n_dupes += len(extras)
    if drop:
        matched = [c for i, c in enumerate(matched) if i not in drop]
        # by_section / by_author counts overshot — recompute from surviving rows.
        by_section = Counter()
        by_author = Counter()
        for c in matched:
            if c.get('match_url'):
                by_section[c['match_section']] += 1
                for au in c.get('match_authors') or []:
                    by_author[au] += 1
        matched_n -= n_dupes
    print(f'Merged duplicates: {n_dupes} secondary notifications folded into primary')

    with open(OUT_MATCHED, 'w') as fh:
        json.dump(matched, fh, separators=(',', ':'))
    print(f'Saved {OUT_MATCHED}')

    summary = {
        'total': len(matched),       # post-dedup row count (what the table shows)
        'parsed': len(corrections),  # raw parsed corrections (pre-dedup)
        'matched': matched_n,
        'by_section': by_section.most_common(50),
        'by_author': by_author.most_common(50),
    }
    with open(OUT_SUMMARY, 'w') as fh:
        json.dump(summary, fh, indent=2)
    print(f'Saved {OUT_SUMMARY}')

    print('\nTop 10 corrected sections:')
    for s, n in by_section.most_common(10):
        print(f'  {s:25s} | {n}')
    print('\nTop 10 corrected authors:')
    for a, n in by_author.most_common(10):
        print(f'  {a:30s} | {n}')


if __name__ == '__main__':
    main()
