"""Normalize stale subject tags across articles, authors, and beats.

The NYT changed several subject-tag labels over time (e.g. "Blacks" →
"Black People", "Illegal Immigrants" → "Illegal Immigration"). build_data.py
maps these at ingestion via _SUBJECT_KW_MERGES, but the on-disk JSON files
were generated before those merges were added. This script:

  1. Applies all merges to the `sb` (subjects) arrays in every articles_YYYY.json
  2. Applies the same merges to the `beats` arrays in authors.json
  3. Rebuilds beats.json from the now-consistent article data

Run from the project root:
    python3 scripts/patch_beats.py
"""
import json, os, glob, math, re
from collections import defaultdict, Counter

DATA_DIR = 'data'

# ── Merge maps (keep in sync with build_data.py _SUBJECT_KW_MERGES) ──────────
_SUBJECT_KW_MERGES = {
    'Housing': 'Real Estate and Housing (Residential)',
    'ATOMIC WEAPONS': 'Nuclear Weapons',
    'UNITED STATES ARMAMENT AND DEFENSE': 'Armament, Defense and Military Forces',
    'RECORDINGS (AUDIO)': 'Recordings and Downloads (Audio)',
    'RECORDINGS (VIDEO)': 'Recordings and Downloads (Video)',
    'APPAREL': 'Fashion and Apparel',
    'RETAIL STORES AND TRADE': 'Shopping and Retail',
    'LABOR': 'Labor and Jobs',
    'IMMIGRATION AND REFUGEES': 'Immigration and Emigration',
    'Illegal Aliens': 'Illegal Immigration',
    'Illegal Immigrants': 'Illegal Immigration',
    'Blacks': 'Black People',
    'Homosexuality': 'Homosexuality and Bisexuality',
    'Transgender': 'Transgender and Transsexuals',
    'Transgender and Transsexual': 'Transgender and Transsexuals',
    'Fentanyl (Drug)': 'Fentanyl',
    'ADVERTISING': 'Advertising and Marketing',
    'Children and Youth': 'Children and Childhood',
    'Demonstrations and Riots': 'Demonstrations, Protests and Riots',
    'Demonstrations, Protests, and Riots': 'Demonstrations, Protests and Riots',
    'Murders and Attempted Murders': 'Murders, Attempted Murders and Homicides',
    'Education and Schools': 'Education (K-12)',
    'Banks and Banking': 'Banking and Financial Institutions',
    'Freedom and Human Rights': 'Human Rights and Human Rights Violations',
    'Suspensions, Dismissals and Resignations': 'Dismissals, Suspensions and Resignations',
}

_ORG_KW_MERGES = {
    'NEW YORK KNICKERBOCKERS': 'New York Knicks',
    'Facebook Inc': 'Meta Platforms Inc',
    'Facebook.com': 'Meta Platforms Inc',
}

def _normalize(name):
    if name in _SUBJECT_KW_MERGES:
        return _SUBJECT_KW_MERGES[name]
    if name in _ORG_KW_MERGES:
        return _ORG_KW_MERGES[name]
    # Title-case ALL-CAPS multi-word tags
    alpha = [c for c in name if c.isalpha()]
    if alpha and all(c.isupper() for c in alpha) and (' ' in name or ',' in name):
        title = name.title()
        for word in (' And ', ' Or ', ' The ', ' Of ', ' In ', ' For ', ' To ', ' A '):
            title = title.replace(word, word.lower())
        return title
    return name


# ── Filter helpers (keep in sync with build_data.py) ─────────────────────────
_GENERIC_SUBJECTS = {
    'United States Politics and Government', 'Content Type: Personal Profile',
    'Content Type: Service', 'your-feed-science', 'your-feed-healthcare',
    'your-feed-internet', 'your-feed-animals', 'your-feed-weather',
    'States (US)', 'Research',
}
_GENERIC_PREFIXES = ('internal-', 'audio-', 'vis-', 'your-feed', 'live-', 'durable-uri')

def _is_generic(s):
    return s in _GENERIC_SUBJECTS or any(s.startswith(p) for p in _GENERIC_PREFIXES)

_INSTITUTIONAL_BYLINES = {
    'The New York Times', 'The Associated Press', 'The Editorial Board',
    'The Learning Network', 'New York Times Games', 'International Herald Tribune',
    'Reuters', 'The New York Times Books Staff', 'The Upshot Staff',
    'The Staff', 'The Times Insider Staff', 'The New York Times Sports Staff',
    'The New York Times Staff',
    'Bloomberg News', 'Associated Press', 'Bridge News', 'Field Level Media',
    'Der Spiegel', 'der Spiegel', 'The International Herald Tribune',
    'Br International Herald',
    'New York Times', 'New York Times Audio', 'New York Times Opinion',
    'The New York Times Opinion', 'The New York Times Magazine',
    'The Styles Desk', 'Retro Report', 'New York Times Cooking',
    'Insider Staff', 'the staff of The Morning',
    'Compiled by The New York Times',
    'IFC Films',
    'Written Mr', 'Was Written Mr',
    'Wire Reports', 'From Wire Reports',
    'T Magazine',
    'Courtesy of NBC', 'Courtesy of CBS', 'Courtesy of ABC',
    'From THE NEW YORK TIMES ALMANAC 2004',
}


# ── Step 1: normalize articles ────────────────────────────────────────────────
print('Step 1: normalizing subjects in articles_YYYY.json files…')
article_files = sorted(glob.glob(os.path.join(DATA_DIR, 'articles_*.json')))
all_articles = []   # we'll need these for beats rebuild
total_changed = 0

for fpath in article_files:
    with open(fpath) as fh:
        arts = json.load(fh)
    changed = 0
    for a in arts:
        raw = a.get('sb') or []
        if not raw:
            continue
        normed = []
        seen = set()
        for s in raw:
            n = _normalize(s)
            if n not in seen:
                normed.append(n)
                seen.add(n)
        if normed != raw:
            a['sb'] = normed
            changed += 1
    all_articles.extend(arts)
    if changed:
        with open(fpath, 'w') as fh:
            json.dump(arts, fh, separators=(',', ':'))
        total_changed += changed
    print(f'  {os.path.basename(fpath)}: {changed} articles updated')

print(f'  Total articles updated: {total_changed}')


# ── Step 2: normalize author beat chips ───────────────────────────────────────
print('\nStep 2: normalizing beat labels in authors.json…')
authors_path = os.path.join(DATA_DIR, 'authors.json')
with open(authors_path) as fh:
    authors = json.load(fh)

authors_updated = 0
for a in authors:
    beats = a.get('beats') or []
    if not beats:
        continue
    normed = []
    seen = set()
    for b in beats:
        n = _normalize(b)
        if n not in seen:
            normed.append(n)
            seen.add(n)
    if normed != beats:
        a['beats'] = normed
        authors_updated += 1

with open(authors_path, 'w') as fh:
    json.dump(authors, fh, separators=(',', ':'))
print(f'  {authors_updated} author records updated')


# ── Step 3: rebuild beats.json from normalized articles ───────────────────────
print('\nStep 3: rebuilding beats.json from normalized article data…')

author_section = {a['name']: a.get('primary_section', '') for a in authors}
corpus_freq = Counter()
corpus_docs = len(all_articles)

for art in all_articles:
    seen = set()
    for s in (art.get('sb') or []):
        if not _is_generic(s) and s not in seen:
            corpus_freq[s] += 1
            seen.add(s)

by_author = defaultdict(list)
for art in all_articles:
    for name in (art.get('a') or []):   # compact key 'a' = authors
        by_author[name].append(art)

subject_index = defaultdict(list)
author_beats_map = {}

for name, arts in by_author.items():
    if name in _INSTITUTIONAL_BYLINES:
        continue
    section = author_section.get(name, '')
    n = len(arts)
    freq = Counter()
    for art in arts:
        seen = set()
        for s in (art.get('sb') or []):
            if not _is_generic(s) and s not in seen:
                freq[s] += 1
                seen.add(s)

    for subj, count in freq.items():
        if count >= 3:
            subject_index[subj].append({'name': name, 'count': count, 'total': n, 'section': section})

    threshold = max(2, math.ceil(n * 0.03))
    scored = []
    for subj, count in freq.items():
        if count < threshold:
            continue
        corpus_count = corpus_freq.get(subj, 1)
        author_rate = count / n
        corpus_rate = corpus_count / corpus_docs
        ratio = author_rate / corpus_rate
        if ratio < 2:
            continue
        score = ratio * math.log(count + 1)
        scored.append((subj, score))
    scored.sort(key=lambda x: -freq[x[0]])
    author_beats_map[name] = [s for s, _ in scored[:7]]

for subj in subject_index:
    subject_index[subj].sort(key=lambda x: x['count'], reverse=True)

subject_list = [
    {'subject': s, 'docCount': corpus_freq[s], 'reporters': len(subject_index[s])}
    for s in subject_index
]
subject_list.sort(key=lambda x: x['docCount'], reverse=True)

known = set(subject_index.keys())
cooccur = defaultdict(Counter)
for art in all_articles:
    subs = [s for s in (art.get('sb') or []) if not _is_generic(s) and s in known]
    for s1 in subs:
        for s2 in subs:
            if s2 != s1:
                cooccur[s1][s2] += 1
cooccur_top = {
    s: [[k, v] for k, v in c.most_common(15)]
    for s, c in cooccur.items() if s in known
}

beats_json = {
    'subjectList': subject_list,
    'subjectIndex': dict(subject_index),
    'corpusSubjectFreq': dict(corpus_freq),
    'corpusSubjectDocs': corpus_docs,
    'cooccur': cooccur_top,
}

beats_path = os.path.join(DATA_DIR, 'beats.json')
with open(beats_path, 'w') as fh:
    json.dump(beats_json, fh, separators=(',', ':'))
print(f'  {len(subject_list)} subjects in rebuilt beats.json')


# ── Step 4: write normalized beats back into authors.json ─────────────────────
print('\nStep 4: updating authors.json beat lists from rebuilt beats…')
updated = 0
for a in authors:
    name = a['name']
    new_beats = author_beats_map.get(name, [])
    if a.get('beats') != new_beats:
        a['beats'] = new_beats
        updated += 1

with open(authors_path, 'w') as fh:
    json.dump(authors, fh, separators=(',', ':'))
print(f'  {updated} author beat lists refreshed')

print('\nDone. Verify with: python3 scripts/patch_beats.py --check (not implemented)')
print('Next: python3 scripts/build_obituaries.py')
