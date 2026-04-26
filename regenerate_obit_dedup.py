"""Surgical regenerator for obituaries.json — re-applies improved dedup.

Operates on existing data/obituaries.json (no raw-dump rebuild). Catches
two known misses:

  1. Same first+last but middle-initial differs (Theodore Kupferman vs
     Theodore R. Kupferman, 1 day apart).
  2. Two republished entries with no surviving "original" record (Judy
     Garland — died 1969, republished 2016 + 2026).

Also drops any entries whose URL is in the build_obituaries.py
NON_OBIT_URLS list (so newly-blocklisted URLs like capote-obits.html
disappear without a raw-dump rebuild).
"""
import json, re, unicodedata
from datetime import date as _date
from collections import defaultdict

# Mirror the dedup helpers from build_obituaries.py
_PREF = {'Obituary (Obit)': 0, 'Obituary': 1, 'Obituary; Biography': 2}
def _rank(o):
    return (_PREF.get(o.get('tom') or '', 9),
            -len(o.get('headline') or ''),
            -len(o.get('url') or ''))

def _parse_date(s):
    try: return _date(int(s[:4]), int(s[5:7]), int(s[8:10]))
    except Exception: return None

def _norm_name(name):
    if not name: return ''
    n = unicodedata.normalize('NFKD', name)
    n = ''.join(c for c in n if not unicodedata.combining(c))
    n = re.sub(r'^(?:Mr|Mrs|Ms|Mx|Dr|Prof|Sir|Lord|Lady|Cardinal|Bishop|Rev|Sister|Father|Brother|Sen|Rep|Gov|Pres|Capt|Col|Gen|Maj|Lt|Sgt|Hon)\.?\s+', '', n, flags=re.I)
    n = re.sub(r',?\s+(?:Jr|Sr|II|III|IV|V|Esq|MD|PhD|DDS)\.?\s*$', '', n, flags=re.I)
    n = re.sub(r"['\u2018\u2019.\-]", '', n)
    n = re.sub(r'\s+', ' ', n).strip().lower()
    toks = [t for t in n.split() if len(t) > 1]
    return ' '.join(toks)

# Pull NON_OBIT_URLS from build_obituaries.py
import importlib.util
spec = importlib.util.spec_from_file_location('build_obituaries', 'build_obituaries.py')
bo = importlib.util.module_from_spec(spec); spec.loader.exec_module(bo)
NON_OBIT_URLS = bo.NON_OBIT_URLS

with open('data/obituaries.json') as f:
    doc = json.load(f)
entries = doc['entries'] if isinstance(doc, dict) else doc
n_in = len(entries)

# 1. Drop newly-blocklisted URLs and rescue any of their secondary_urls
# that aren't blocklisted (rare, defensive).
kept = []
n_dropped = 0
for e in entries:
    u = e.get('url') or ''
    if u in NON_OBIT_URLS:
        n_dropped += 1
        continue
    kept.append(e)
print(f"Dropped {n_dropped} entries whose URL is in NON_OBIT_URLS")

# 2. Re-cluster ±10 days using normalized name (catches Kupferman).
by_norm = defaultdict(list)
no_name = []
for e in kept:
    if not e.get('name'):
        no_name.append(e); continue
    by_norm[_norm_name(e['name']) or e['name']].append(e)

merged = []
n_clust = 0
for key, recs in by_norm.items():
    recs_sorted = sorted(recs, key=lambda r: r.get('date') or '')
    clusters = []
    for r in recs_sorted:
        d = _parse_date(r.get('date') or '')
        if not clusters:
            clusters.append([r]); continue
        last = clusters[-1][-1]
        d2 = _parse_date(last.get('date') or '')
        if d and d2 and abs((d - d2).days) <= 10:
            clusters[-1].append(r)
        else:
            clusters.append([r])
    for cluster in clusters:
        if len(cluster) == 1:
            merged.append(cluster[0])
        else:
            primary = sorted(cluster, key=_rank)[0]
            others = [c for c in cluster if c is not primary]
            sec_urls = list(primary.get('secondary_urls') or [])
            sec_dates = list(primary.get('secondary_dates') or [])
            for c in others:
                if c.get('url'): sec_urls.append(c.get('url'))
                if c.get('date'): sec_dates.append(c.get('date'))
                # also pull any secondary URLs the dropped record itself had
                sec_urls.extend(c.get('secondary_urls') or [])
                sec_dates.extend(c.get('secondary_dates') or [])
            # de-dupe sec arrays preserving order
            seen=set(); su=[]; sd=[]
            for u, d in zip(sec_urls, sec_dates + ['']*max(0,len(sec_urls)-len(sec_dates))):
                if u and u != primary.get('url') and u not in seen:
                    seen.add(u); su.append(u); sd.append(d)
            if su:
                primary['secondary_urls'] = su
                primary['secondary_dates'] = sd
            merged.append(primary)
            n_clust += len(others)
merged.extend(no_name)
print(f"Clustered {n_clust} same-name (normalized) ±10-day duplicates")

# 3. Repub→orig and repub→repub merges (Garland case).
by_norm2 = defaultdict(list)
for e in merged:
    nm = e.get('name')
    if nm: by_norm2[_norm_name(nm) or nm].append(e)
n_repub_merged = 0
drop_ids = set()
for key, recs in by_norm2.items():
    if len(recs) < 2: continue
    repubs = [r for r in recs if r.get('republished')]
    origs  = [r for r in recs if not r.get('republished')]
    if origs and repubs:
        primary = sorted(origs, key=_rank)[0]
        others = repubs
    elif len(repubs) >= 2 and not origs:
        rs = sorted(repubs, key=lambda r: r.get('date') or '')
        primary = rs[0]; others = rs[1:]
    else:
        continue
    sec_urls = list(primary.get('secondary_urls') or [])
    sec_dates = list(primary.get('secondary_dates') or [])
    for r in others:
        if r.get('url'): sec_urls.append(r.get('url'))
        if r.get('date'): sec_dates.append(r.get('date'))
        drop_ids.add(id(r))
        n_repub_merged += 1
    seen=set(); su=[]; sd=[]
    for u, d in zip(sec_urls, sec_dates + ['']*max(0,len(sec_urls)-len(sec_dates))):
        if u and u != primary.get('url') and u not in seen:
            seen.add(u); su.append(u); sd.append(d)
    if su:
        primary['secondary_urls'] = su
        primary['secondary_dates'] = sd

merged = [e for e in merged if id(e) not in drop_ids]
print(f"Repub-cluster merge: {n_repub_merged} dropped onto canonical")

print(f"Total: {n_in:,} → {len(merged):,}")

# Write back, preserving wrapper structure
if isinstance(doc, dict):
    doc['entries'] = merged
    out = doc
else:
    out = merged
with open('data/obituaries.json', 'w') as f:
    json.dump(out, f, ensure_ascii=False, separators=(',', ':'))
print("Wrote data/obituaries.json")
