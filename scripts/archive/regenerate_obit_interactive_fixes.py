"""Surgical regenerator for /interactive/ obit name/profession swaps.

Re-applies NON_OBIT_URLS filtering and OBIT_OVERRIDES to existing
data/obituaries.json without a full build_obituaries.py run.

Targets the 2026-04-25 /interactive/ audit:
  - 5 newly-blocklisted URLs (video galleries, Breaking Bread features)
  - 15 OBIT_OVERRIDES adding name/profession/gender for obit records
    where the parser swapped or truncated fields (Bruce Lee was the canary).
"""
import json, importlib.util

import os
_HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location('build_obituaries', os.path.join(_HERE, 'build_obituaries.py'))
bo = importlib.util.module_from_spec(spec); spec.loader.exec_module(bo)
NON_OBIT_URLS = bo.NON_OBIT_URLS
OBIT_OVERRIDES = bo.OBIT_OVERRIDES

with open('data/obituaries.json') as f:
    doc = json.load(f)
entries = doc['entries'] if isinstance(doc, dict) else doc
n_in = len(entries)

# 1. Drop blocklisted URLs.
kept, n_dropped = [], 0
for e in entries:
    u = e.get('url') or ''
    if u in NON_OBIT_URLS:
        n_dropped += 1
        continue
    kept.append(e)
print(f"Dropped {n_dropped} entries whose URL is in NON_OBIT_URLS")

# 2. Apply OBIT_OVERRIDES (name/profession/gender/age) to surviving records.
n_overridden = 0
for e in kept:
    u = e.get('url') or ''
    if u in OBIT_OVERRIDES:
        ov = OBIT_OVERRIDES[u]
        changed = False
        for k, v in ov.items():
            if e.get(k) != v:
                e[k] = v
                changed = True
        if 'name' in ov:
            # display_name mirrors name when no honorific prefix logic applies.
            # For these /interactive/ records there's no original display logic
            # to recover; keep display_name in sync with name.
            new_dn = ov['name']
            if e.get('display_name') != new_dn:
                e['display_name'] = new_dn
                changed = True
        if changed:
            n_overridden += 1
print(f"Applied OBIT_OVERRIDES to {n_overridden} records")

print(f"Total: {n_in:,} → {len(kept):,}")

# Write back, preserving wrapper structure.
if isinstance(doc, dict):
    doc['entries'] = kept
    out = doc
else:
    out = kept
with open('data/obituaries.json', 'w') as f:
    json.dump(out, f, ensure_ascii=False, separators=(',', ':'))
print("Wrote data/obituaries.json")
