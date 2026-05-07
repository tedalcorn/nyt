"""Normalize stale subject tags across articles, authors, and beats.

The NYT changed several subject-tag labels over time (e.g. "Blacks" →
"Black People", "Illegal Immigrants" → "Illegal Immigration"). build_data.py
maps these at ingestion via tag_config.json, but on-disk JSON files may have
been generated before merges were applied. This script:

  1. Applies all merges to the `sb` (subjects) arrays in every articles_YYYY.json
  2. Applies the same merges to the `beats` arrays in authors.json
  3. Rebuilds beats.json from the now-consistent article data

Run from the project root:
    python3 scripts/patch_beats.py
"""
import json, os, glob, math, re
from collections import defaultdict, Counter

DATA_DIR = 'data'

# Load shared tag configuration (keep in sync with build_data.py)
with open(os.path.join(DATA_DIR, 'tag_config.json')) as f:
    TAG_CONFIG = json.load(f)

_SUBJECT_KW_MERGES = TAG_CONFIG.get('subject_merges', {})
_ORG_KW_MERGES = TAG_CONFIG.get('org_merges', {})

_ABBREVS = TAG_CONFIG.get('abbrev_fixes', [])
_ABBREV_TITLES = sorted({a.title() for a in _ABBREVS}, key=lambda x: -len(x))
_ABBREV_RE = re.compile(
    r'(?<=[\s,(\-])(' + '|'.join(re.escape(t) for t in _ABBREV_TITLES) + r')(?=[\s,)\-]|$)'
) if _ABBREV_TITLES else None

def _restore_abbrevs(name):
    if not _ABBREV_RE:
        return name
    return _ABBREV_RE.sub(lambda m: m.group(1).upper(), name)


_LEGACY_UNDERSCORE_RE = re.compile(r'^[a-z][a-z_]*$')

def _normalize(name):
    """Mirror _normalize_subject_kw in build_data.py: drop legacy underscore
    tags, apply explicit merges, auto-titlecase for ALL-CAPS tags (skip
    periods/apostrophes), then restore state/country abbrevs that
    str.title() mangles."""
    if _LEGACY_UNDERSCORE_RE.match(name) and '_' in name:
        return None  # drop legacy underscore-style tags
    if name in _SUBJECT_KW_MERGES:
        return _SUBJECT_KW_MERGES[name]
    if name in _ORG_KW_MERGES:
        return _ORG_KW_MERGES[name]
    restored = _restore_abbrevs(name)
    if restored != name:
        if restored in _SUBJECT_KW_MERGES:
            return _SUBJECT_KW_MERGES[restored]
        if restored in _ORG_KW_MERGES:
            return _ORG_KW_MERGES[restored]
        name = restored
    alpha = [c for c in name if c.isalpha()]
    if alpha and all(c.isupper() for c in alpha) and '.' not in name and "'" not in name and '’' not in name:
        title = name.title()
        for word in (' And ', ' Or ', ' The ', ' Of ', ' In ', ' For ', ' To ', ' A '):
            title = title.replace(word, word.lower())
        title = _restore_abbrevs(title)
        if title in _SUBJECT_KW_MERGES:
            return _SUBJECT_KW_MERGES[title]
        if title in _ORG_KW_MERGES:
            return _ORG_KW_MERGES[title]
        return title
    return name


# Filter helpers (loaded from tag_config.json)
_GENERIC_SUBJECTS = set(TAG_CONFIG.get('generic_subjects_always_filter', []))
_GENERIC_PREFIXES = tuple(TAG_CONFIG.get('generic_prefixes_always_filter', []))

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
            if n is None:  # legacy underscore tag — drop
                continue
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
        if n is None:
            continue
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
