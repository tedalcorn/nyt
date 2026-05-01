"""Surgical regenerator for the `world_coverage` block in dashboard.json.

Imports the canonical normalization (LOCATION_NORMALIZE, PARENT_MAP, DROP_LOCS,
_normalize_loc) from build_data.py — the same logic the full build uses — and
applies one override on top: Greenland is NOT collapsed into Denmark.

Why this exists: a full build_data.py run takes several minutes and rebuilds
~30MB of artifacts. Just the geo aggregation is ~30 seconds, so we patch
dashboard.json in place rather than triggering a full rebuild.

Earlier versions of this script duplicated _normalize_loc in stripped form,
which silently regressed when build_data.py got upgrades (ALL CAPS handling,
expanded LOCATION_NORMALIZE, US-state PARENT_MAP entries). Importing the
canonical helpers prevents that drift.
"""
import json
import glob
import os
import importlib.util
from collections import Counter, defaultdict


# Load build_data.py as a module to reuse its normalization logic.
_spec = importlib.util.spec_from_file_location('build_data', 'build_data.py')
_bd = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_bd)

# Override Greenland routing so it surfaces as its own polygon on the map.
# The override only affects this regenerator — build_data.py's module dict is
# patched in place for this process and is not written back to disk.
_bd.LOCATION_NORMALIZE.pop('Greenland', None)
_bd.LOCATION_NORMALIZE.pop('Nuuk (Greenland)', None)
_bd.PARENT_MAP.pop('Greenland', None)
_bd.LOCATION_NORMALIZE['Nuuk (Greenland)'] = 'Greenland'

normalize_loc = _bd._normalize_loc


def main():
    print("Reading 315 raw monthly dumps...")
    glocation_year = defaultdict(lambda: defaultdict(int))
    glocation_total = Counter()
    region_year = defaultdict(lambda: defaultdict(int))
    raw_files = sorted(glob.glob('data/raw/*.json'))
    for i, f in enumerate(raw_files, 1):
        if i % 50 == 0:
            print(f"  {i}/{len(raw_files)} ({f})")
        d = json.load(open(f))
        docs = d if isinstance(d, list) else d.get('docs') or d.get('response', {}).get('docs', [])
        for art in docs:
            section = (art.get('section_name') or art.get('section') or '').strip()
            if section != 'World':
                continue
            pub = (art.get('pub_date') or '')[:10]
            year = pub[:4] if pub else ''
            if not year:
                continue
            sub = (art.get('subsection_name') or art.get('subsection') or '').strip()
            if sub:
                region_year[sub][year] += 1
            for k in art.get('keywords') or []:
                kn = k.get('name') or ''
                if kn in ('glocations', 'Location'):
                    raw_loc = k.get('value') or ''
                    loc = normalize_loc(raw_loc)
                    if loc:
                        glocation_year[loc][year] += 1
                        glocation_total[loc] += 1

    # Top locations: ≥5 articles total
    top_locs = [loc for loc, cnt in glocation_total.most_common() if cnt >= 5]

    new_world_coverage = {
        'locations': top_locs,
        'location_trends': {loc: dict(glocation_year[loc]) for loc in top_locs},
        'region_trends': {r: dict(region_year[r]) for r in sorted(region_year.keys())},
        'years': sorted({y for trends in glocation_year.values() for y in trends.keys()}),
    }

    # Patch dashboard.json — preserve all other fields, replace world_coverage
    print("\nLoading dashboard.json...")
    dashboard_path = 'data/dashboard.json'
    with open(dashboard_path) as f:
        dash = json.load(f)
    old = dash.get('world_coverage', {})
    print(f"  old world_coverage: {len(old.get('locations', []))} locations")
    print(f"  new world_coverage: {len(top_locs)} locations")
    # Preserve blog_location_trends if present (we don't recompute it here)
    if 'blog_location_trends' in old:
        new_world_coverage['blog_location_trends'] = old['blog_location_trends']
    dash['world_coverage'] = new_world_coverage
    with open(dashboard_path, 'w') as f:
        json.dump(dash, f, separators=(',', ':'))
    sz = os.path.getsize(dashboard_path)
    print(f"  patched dashboard.json ({sz:,} bytes)")
    print()
    print(f"Greenland total: {glocation_total['Greenland']}")
    print(f"Greenland 2025: {glocation_year['Greenland'].get('2025', 0)}")
    print(f"Denmark total: {glocation_total['Denmark']}")
    print(f"Denmark 2025: {glocation_year['Denmark'].get('2025', 0)}")
    print(f"Antarctica total: {glocation_total['Antarctica']}")


if __name__ == '__main__':
    main()
