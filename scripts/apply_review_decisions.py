"""Apply corrections review decisions from the browser-exported overrides file.

Usage:
  1. In review_corrections.html, click "Export overrides JSON"
  2. Save the file as data/corrections_overrides.json
  3. Run: python3 apply_review_decisions.py

Updates corrections_matched.json with accepted matches, then rebuilds
corrections_automatched_review.json to remove any resolved items.
"""
import json, os

os.chdir("/Users/tedalcorn/Desktop/claude-projects/nyt")

MATCHED_PATH = 'data/corrections_matched.json'
OVERRIDES_PATH = 'data/corrections_overrides.json'
REVIEW_PATH = 'data/corrections_automatched_review.json'

if not os.path.exists(OVERRIDES_PATH):
    print(f"Not found: {OVERRIDES_PATH}")
    print("Export your decisions from the review UI first.")
    raise SystemExit(1)

with open(OVERRIDES_PATH) as f:
    overrides = json.load(f)

with open(MATCHED_PATH) as f:
    matched = json.load(f)

with open(REVIEW_PATH) as f:
    review = json.load(f)

decisions = {d['correction_id']: d for d in overrides.get('decisions', [])}
print(f"Loaded {len(decisions)} decisions from overrides file")

# The review UI uses: (page_url || '') + '||' + text.slice(0, 80)
def make_id(page_url, text):
    return (page_url or '') + '||' + (text or '')[:80]

matched_idx = {}
for i, c in enumerate(matched):
    key = make_id(c.get('page_url', ''), c.get('text', ''))
    matched_idx[key] = i

applied = 0
rejected = 0
not_found = 0
for decision_id, d in decisions.items():
    idx = matched_idx.get(decision_id)
    if idx is None:
        not_found += 1
        continue
    c = matched[idx]
    if d['action'] == 'accept' and d.get('url'):
        c['match_url'] = d['url']
        c['match_source'] = 'manual' if d.get('score', 0) < 0 else 'automatch_reviewed'
        c['match_score'] = d.get('score', 0)
        applied += 1
    elif d['action'] == 'reject':
        c['match_url'] = None
        c['match_source'] = 'rejected'
        rejected += 1

print(f"Applied: {applied} accepted, {rejected} rejected, {not_found} not found")

with open(MATCHED_PATH, 'w') as f:
    json.dump(matched, f, ensure_ascii=False, separators=(',', ':'))
print(f"Updated {MATCHED_PATH}")

# Remove resolved items from review queue
resolved_ids = {did for did, d in decisions.items() if d['action'] in ('accept', 'reject')}
review_id_lookup = {make_id(r.get('page_url', ''), r.get('text', '')): r for r in review}
remaining = [r for rid, r in review_id_lookup.items() if rid not in resolved_ids]
with open(REVIEW_PATH, 'w') as f:
    json.dump(remaining, f, ensure_ascii=False, indent=1)
print(f"Review queue: {len(review)} -> {len(remaining)}")
