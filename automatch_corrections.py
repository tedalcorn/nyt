"""Phase 3: auto-match corrections that the build pipeline left unmatched, plus
those whose DOW-inferred print date is far from the matched article's pub date
(dow_match_diff >= 7) — likely wrong matches.

Strategy per correction:
  1. spaCy NER → extract PERSON / GPE / ORG / NORP / FAC / EVENT entities
  2. Anchor date = dow_inferred_date | ref_date | page_date
  3. NYT Article Search API: q=<best entity>, begin/end = anchor ± 5 days
  4. Score candidates: weighted overlap of correction-text tokens with each
     candidate's headline + lead_paragraph + abstract + keyword names

Output: data/corrections_automatched_review.json — list of {original, candidates}
        with all candidates included so the human reviewer sees the full pool.

Idempotent. Caches Article Search responses in cache/automatch/<slug>.json so
re-runs don't re-burn API quota.

Throttle: 0.6s between requests = ~100 req/min, well under NYT's 5 req/sec.
"""
import os, sys, json, time, re, hashlib, urllib.parse, urllib.request
from datetime import date, timedelta

API_KEY = os.environ.get('NYT_API_KEY')
if not API_KEY:
    print('Set NYT_API_KEY in env'); sys.exit(1)

CACHE_DIR = 'cache/automatch'
OUT_PATH = 'data/corrections_automatched_review.json'
os.makedirs(CACHE_DIR, exist_ok=True)

SEARCH_URL = 'https://api.nytimes.com/svc/search/v2/articlesearch.json'
PACE = 0.6   # seconds between calls
WINDOW_DAYS = 5

# Entity types we'll use, in priority order
ENT_PRIORITY = ['PERSON', 'ORG', 'GPE', 'NORP', 'FAC', 'EVENT', 'WORK_OF_ART']


def slug(s):
    return hashlib.md5(s.encode()).hexdigest()[:16]


def cache_path(query, begin, end):
    key = f'{query}|{begin}|{end}'
    return os.path.join(CACHE_DIR, slug(key) + '.json')


def search_articles(query, begin, end):
    """NYT Article Search. Cached on (query, begin, end). Returns list of docs."""
    cp = cache_path(query, begin, end)
    if os.path.exists(cp):
        with open(cp) as f:
            return json.load(f)
    params = {
        'q': query,
        'begin_date': begin.replace('-', ''),
        'end_date': end.replace('-', ''),
        'api-key': API_KEY,
        'sort': 'relevance',
    }
    url = SEARCH_URL + '?' + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'NYTCorrectionsAutomatch/1.0'})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.load(r)
    except urllib.error.HTTPError as e:
        if e.code == 429:
            print('  HIT 429 rate limit — sleeping 60s')
            time.sleep(60)
            return search_articles(query, begin, end)
        if e.code in (401, 403):
            body = e.read()[:300].decode('utf-8', errors='replace')
            print(f'\nFATAL: NYT API returned {e.code} {e.reason}')
            print(f'  body: {body}')
            print('  Article Search API is a separate product on developer.nytimes.com')
            print('  → enable "Article Search API" on your app at https://developer.nytimes.com/my-apps')
            sys.exit(1)
        print(f'  HTTP {e.code} on q={query!r}: {e.reason}')
        return []
    except Exception as e:
        print(f'  search error on q={query!r}: {e}')
        return []
    docs = (data.get('response') or {}).get('docs') or []
    # Trim to what we need so cache files stay small
    trimmed = []
    for d in docs:
        h = (d.get('headline') or {}).get('main') or ''
        url = d.get('web_url') or ''
        path = re.sub(r'^https?://[^/]+', '', url)
        kw = [k.get('value') for k in (d.get('keywords') or []) if k.get('value')]
        trimmed.append({
            'url': path,
            'web_url': url,
            'pub_date': (d.get('pub_date') or '')[:10],
            'section': d.get('section_name') or '',
            'subsection': d.get('subsection_name') or '',
            'headline': h,
            'lead_paragraph': (d.get('lead_paragraph') or '')[:400],
            'abstract': (d.get('abstract') or '')[:400],
            'byline': (d.get('byline') or {}).get('original') or '',
            'keywords': kw,
            'word_count': d.get('word_count') or 0,
            'type_of_material': d.get('type_of_material') or '',
        })
    with open(cp, 'w') as f:
        json.dump(trimmed, f)
    time.sleep(PACE)
    return trimmed


# ── Scoring ─────────────────────────────────────────────────────────────────
TOK_RE = re.compile(r"[A-Za-z][A-Za-z'\-]{2,}")
STOPWORDS = set("""
a an the and or but if then else when with from for to of in on at by as is
was were be been being have has had do does did this that these those it its
which who whom whose what where why how than not no nor so very also more most
some all any one two three any can may might must will shall would should could
there here their your our his her him she they them you us we i article item
correction obituary picture caption headline review chart map listing news
yesterday today thursday friday monday tuesday wednesday saturday sunday last
about because of due to earlier version misstated misidentified incorrectly
referred misspelled gave wrong should have stated read incorrect among other
not been said only because misstating misstating because referred mention
""".split())


def tokens(s):
    return [t.lower() for t in TOK_RE.findall(s or '') if t.lower() not in STOPWORDS]


def score_candidate(corr_text, corr_entities, anchor_date, candidate):
    """Return (score, breakdown_dict)."""
    cand_text = ' '.join([
        candidate.get('headline', ''),
        candidate.get('lead_paragraph', ''),
        candidate.get('abstract', ''),
        ' '.join(candidate.get('keywords', []) or []),
    ])
    corr_toks = set(tokens(corr_text))
    cand_toks = set(tokens(cand_text))
    overlap_toks = corr_toks & cand_toks

    # Entity bonus — exact-string presence of correction entities in candidate text
    cand_lc = cand_text.lower()
    ent_hits = [e for e in corr_entities if e.lower() in cand_lc]
    ent_score = sum(len(e.split()) for e in ent_hits)   # multi-word entities count more

    # Date proximity (closer = better, max bonus at 0 days)
    days = None
    if anchor_date and candidate.get('pub_date'):
        try:
            days = abs((date.fromisoformat(anchor_date) - date.fromisoformat(candidate['pub_date'])).days)
        except Exception:
            days = None
    date_score = max(0, (WINDOW_DAYS + 1) - days) if days is not None else 0

    # Type penalty: skip Correction self-references
    tom = candidate.get('type_of_material', '')
    if tom == 'Correction':
        type_penalty = -10
    elif tom in ('Slideshow', 'Video', 'Audio'):
        type_penalty = -5
    else:
        type_penalty = 0

    score = (3.0 * ent_score) + (1.0 * len(overlap_toks)) + (0.5 * date_score) + type_penalty
    return score, {
        'ent_hits': ent_hits,
        'overlap_tokens': sorted(overlap_toks)[:12],
        'days_off': days,
        'type_penalty': type_penalty,
    }


# ── Main pipeline ───────────────────────────────────────────────────────────
def get_anchor(c):
    return c.get('dow_inferred_date') or c.get('ref_date') or c.get('page_date')


def extract_entities(nlp, text):
    if not text:
        return []
    doc = nlp(text)
    by_type = {}
    for ent in doc.ents:
        if ent.label_ not in ENT_PRIORITY:
            continue
        s = ent.text.strip()
        if len(s) < 3 or s.lower() in STOPWORDS:
            continue
        # Strip leading "the "
        s = re.sub(r'^the\s+', '', s, flags=re.I)
        by_type.setdefault(ent.label_, []).append(s)
    # Return ordered by priority + length
    out = []
    for t in ENT_PRIORITY:
        for s in by_type.get(t, []):
            if s not in out:
                out.append(s)
    return out


def confidence_band(score):
    if score >= 12: return 'high'
    if score >= 6: return 'medium'
    return 'low'


def process_correction(c, nlp):
    text = c.get('text') or ''
    anchor = get_anchor(c)
    entities = extract_entities(nlp, text)
    queries_tried = []
    candidates = []
    if anchor and entities:
        d = date.fromisoformat(anchor)
        begin = (d - timedelta(days=WINDOW_DAYS)).isoformat()
        end = (d + timedelta(days=WINDOW_DAYS)).isoformat()
        # Try the top entity, then the next as a fallback (if first returns nothing)
        for q in entities[:2]:
            queries_tried.append(q)
            docs = search_articles(q, begin, end)
            for d_ in docs:
                if d_['url'] in [c2['url'] for c2 in candidates]:
                    continue
                score, breakdown = score_candidate(text, entities, anchor, d_)
                d_ = {**d_, 'score': round(score, 2), 'band': confidence_band(score),
                      'breakdown': breakdown, 'matched_via_query': q}
                candidates.append(d_)
            if len(docs) >= 1:
                break  # if first query returned anything, don't burn a second call
    candidates.sort(key=lambda d: -d['score'])
    return {
        'page_date': c.get('page_date'),
        'page_url': c.get('page_url'),
        'text': c.get('text'),
        'ref_date': c.get('ref_date'),
        'dow_inferred_date': c.get('dow_inferred_date'),
        'dow_name': c.get('dow_name'),
        'anchor_date': anchor,
        'entities': entities,
        'queries_tried': queries_tried,
        'existing_match_url': c.get('match_url'),
        'existing_match_score': c.get('match_score'),
        'existing_match_section': c.get('match_section'),
        'existing_match_headline': c.get('match_headline'),
        'existing_match_authors': c.get('match_authors'),
        'dow_match_diff': c.get('dow_match_diff'),
        'reason': 'unmatched' if not c.get('match_url') else 'high_dow_diff',
        'candidates': candidates[:5],   # top 5 only — keep file readable
        'best_band': candidates[0]['band'] if candidates else 'none',
    }


def main():
    import spacy
    print('Loading spaCy en_core_web_sm…')
    nlp = spacy.load('en_core_web_sm')

    with open('data/corrections_matched.json') as f:
        cm = json.load(f)

    targets = [c for c in cm
               if not c.get('match_url') or (c.get('dow_match_diff') or 0) >= 7]
    print(f'Targets: {len(targets)} ({sum(1 for c in targets if not c.get("match_url"))} unmatched + '
          f'{sum(1 for c in targets if (c.get("dow_match_diff") or 0) >= 7)} high-DOW-diff)')

    out = []
    band_counts = {'high': 0, 'medium': 0, 'low': 0, 'none': 0}
    for i, c in enumerate(targets):
        rec = process_correction(c, nlp)
        out.append(rec)
        band_counts[rec['best_band']] = band_counts.get(rec['best_band'], 0) + 1
        if (i + 1) % 25 == 0 or i == len(targets) - 1:
            print(f'  [{i+1}/{len(targets)}]  bands so far: {band_counts}')

    with open(OUT_PATH, 'w') as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
    print(f'\nWrote {OUT_PATH} ({os.path.getsize(OUT_PATH):,} bytes)')
    print(f'Final bands: {band_counts}')


if __name__ == '__main__':
    main()
