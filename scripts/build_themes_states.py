"""Precompute per-state tag inventory for the Themes tab (US side).

Output: site/data/themes_states.json
Structure mirrors themes_explorer.json so the same JS can swap between them.
Corpus = U.S. + New York section articles, post-correction, post-listing-filter.
"""
import json
import os
import re
import sys
from collections import Counter, defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SITE_DIR = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(SITE_DIR, 'data')

sys.path.insert(0, os.path.abspath(os.path.join(SITE_DIR, '..', 'projects',
                                                '2026-05-top-keyword',
                                                '2026-05-13-world-country-tweets',
                                                'scripts')))
import build_country_keywords as bck   # reuse subject_merges loader

# Load tag_config — also gives us the state-side filter lists.
with open(os.path.join(DATA_DIR, 'tag_config.json')) as f:
    TAG_CONFIG = json.load(f)

STATE_GENERIC      = set(TAG_CONFIG.get('state_coverage_generic_subjects', []))
STATE_GEN_PREFIXES = list(TAG_CONFIG.get('state_coverage_generic_prefixes', []))
LISTING_EXACT      = set(TAG_CONFIG.get('state_listing_headlines_exact', []))
LISTING_PREFIXES   = tuple(TAG_CONFIG.get('state_listing_headline_prefixes', []))
LISTING_KICKERS    = set(TAG_CONFIG.get('state_listing_kickers_exact', []))
SINGLE_VENUE       = set(TAG_CONFIG.get('single_venue_tags', []))
# No per-state carve-outs — the user-side year-range slider lets people
# narrow past 2001-02 themselves if 9/11 tags dominate.
STATE_TAG_EXCLUSIONS = {}

CORR_RE = re.compile(r'/(c-)?corrections?-|/pageoneplus/corrections-')
def is_corr(a):
    sb = a.get('sb') or []
    if 'Correction Stories' in sb: return True
    if (a.get('s') or '') == 'Corrections': return True
    return bool(CORR_RE.search(a.get('u') or ''))

def is_listing(a):
    h = a.get('h') or ''
    if h in LISTING_EXACT: return True
    if any(h.startswith(p) for p in LISTING_PREFIXES): return True
    return (a.get('k') or '') in LISTING_KICKERS

MIN_STATE_ARTICLES   = 50
MIN_TAG_COUNT_PER_STATE = 3


def main():
    print('Loading articles + applying subject merges…')
    arts = bck.load_world_articles()   # name's misleading; loads everything
    us_arts = [a for a in arts if (a.get('s') or '') in ('U.S.', 'New York')]
    print(f'  U.S. + New York section: {len(us_arts):,}')

    us_clean = [a for a in us_arts
                if not is_corr(a) and not is_listing(a)]
    print(f'  After corrections + listings filter: {len(us_clean):,}')

    print('Computing corpus baseline…')
    corpus_freq = Counter()
    corpus_freq_by_year = defaultdict(Counter)
    corpus_total_by_year = Counter()
    for a in us_clean:
        yr = (a.get('d') or '')[:4]
        if yr: corpus_total_by_year[yr] += 1
        for tag in set(a.get('sb') or []):
            corpus_freq[tag] += 1
            if yr: corpus_freq_by_year[tag][yr] += 1
    corpus_total = len(us_clean)

    print('Tabulating per-state tags…')
    state_total = Counter()
    state_total_by_year = defaultdict(Counter)
    state_tag_counts = defaultdict(Counter)
    state_tag_years = defaultdict(lambda: defaultdict(Counter))
    for a in us_clean:
        states = a.get('st') or []
        yr = (a.get('d') or '')[:4]
        for st in states:
            state_total[st] += 1
            if yr: state_total_by_year[st][yr] += 1
            seen = set()
            for tag in (a.get('sb') or []):
                if tag in seen: continue
                seen.add(tag)
                state_tag_counts[st][tag] += 1
                if yr: state_tag_years[st][tag][yr] += 1

    print(f'  States tracked: {len(state_total)}')

    states = {}
    for st, n in state_total.items():
        if n < MIN_STATE_ARTICLES: continue
        tags = {}
        for tag, cnt in state_tag_counts[st].items():
            if cnt < MIN_TAG_COUNT_PER_STATE: continue
            tags[tag] = {
                'count': cnt,
                'years': dict(state_tag_years[st][tag]),
            }
        states[st] = {
            'total': n,
            'total_by_year': dict(state_total_by_year[st]),
            'tags': tags,
        }
    print(f'  States exported: {len(states)}')

    # Filter exposure for client side
    # We expose state-specific lists plus the generic ones; the JS knows
    # whether it's in countries or states mode and which to apply.
    # Always-on global prefix filter (internal-*, audio-*, etc.) — these
    # come from generic_prefixes_always_filter and apply regardless of scope.
    GLOBAL_PREFIXES = list(TAG_CONFIG.get('generic_prefixes_always_filter', []))
    filters = {
        'generic_always':           TAG_CONFIG.get('generic_subjects_always_filter', []),
        'global_prefixes':          GLOBAL_PREFIXES,
        'state_generic_subjects':   list(STATE_GENERIC),
        'state_generic_prefixes':   STATE_GEN_PREFIXES + GLOBAL_PREFIXES,
        'single_venue_tags':        list(SINGLE_VENUE),
        'state_tag_exclusions':     {k: list(v) for k, v in STATE_TAG_EXCLUSIONS.items()},
        'headline_event_tags':      TAG_CONFIG.get('headline_event_tags', []),
        'headline_event_patterns':  TAG_CONFIG.get('headline_event_patterns', []),
    }

    # State geometries — simplify, drop AK/HI from-bbox handled by silhouette
    print('Embedding state geometries…')
    import geopandas as gpd
    gdf = gpd.read_file(os.path.join(DATA_DIR, 'us_states.geojson'))
    name_field = 'NAME' if 'NAME' in gdf.columns else 'name'
    # Geojson uses "District of Columbia"; analysis uses "D.C."
    geojson_to_analysis = {'District of Columbia': 'D.C.'}

    def simplify_geom(geom, tolerance=0.05):
        try:
            g = geom.simplify(tolerance, preserve_topology=True)
        except Exception:
            g = geom
        polys = list(g.geoms) if g.geom_type == 'MultiPolygon' else [g]
        polys = [p for p in polys if p.exterior]
        if not polys:
            return []
        # Drop far-flung islands (Aleutians for Alaska wrap antimeridian).
        if len(polys) > 1:
            main = max(polys, key=lambda p: p.area)
            mx0, my0, mx1, my1 = main.bounds
            mcx, mcy = (mx0+mx1)/2, (my0+my1)/2
            kept = []
            for p in polys:
                px0, py0, px1, py1 = p.bounds
                pcx, pcy = (px0+px1)/2, (py0+py1)/2
                if abs(pcx-mcx) <= 25 and abs(pcy-mcy) <= 25:
                    kept.append(p)
            if kept: polys = kept
        return [[[round(x, 3), round(y, 3)] for x, y in p.exterior.coords]
                for p in polys]

    state_geometry = {}
    for _, row in gdf.iterrows():
        nm = row[name_field]
        analysis = geojson_to_analysis.get(nm, nm)
        if analysis in states:
            state_geometry[analysis] = simplify_geom(row.geometry)

    used_tags = set()
    for s in states.values():
        used_tags.update(s['tags'].keys())
    corpus_freq_by_year_out = {
        tag: dict(corpus_freq_by_year[tag])
        for tag in used_tags
        if tag in corpus_freq_by_year
    }

    out = {
        'corpus_total': corpus_total,
        'corpus_total_by_year': dict(corpus_total_by_year),
        'corpus_freq': dict(corpus_freq),
        'corpus_freq_by_year': corpus_freq_by_year_out,
        'states': states,
        'filters': filters,
        'state_geometry': state_geometry,
    }

    out_path = os.path.join(DATA_DIR, 'themes_states.json')
    with open(out_path, 'w') as f:
        json.dump(out, f, separators=(',', ':'))
    size = os.path.getsize(out_path) / (1024*1024)
    print(f'Saved {out_path} ({size:.1f} MB)')


if __name__ == '__main__':
    main()
