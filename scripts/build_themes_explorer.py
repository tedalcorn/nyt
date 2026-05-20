"""Precompute per-country tag inventory for the interactive Themes tab.

Output: site/data/themes_explorer.json — flat enough for client-side filtering.
Schema:
{
  "corpus_total": int,          # World-section article count (denominator)
  "corpus_freq": {tag: int},    # tag → World-article count
  "countries": {
    "France": {
      "total": int,             # post-correction country article count
      "tags": {tag: {"count": int, "years": {"2018": 5, ...}}}
    },
    ...
  },
  "filters": {                  # ready-to-use exclusion lists from tag_config
    "world_generic_subjects": [...],
    "world_generic_substrings": [...],
    "world_generic_prefixes": [...],
    "headline_event_tags": [...],
    "headline_event_patterns": [...],
    "country_exclusion_tags": {country: [tags]},
    "country_event_tags": {country: [tags]}
  },
  "country_geometry": {         # silhouette polygon, lat/lon points
    "France": [[[lon,lat], ...], ...]   # list of rings (multipolygon support)
  }
}
"""
import json
import os
import re
import sys
from collections import Counter, defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SITE_DIR = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(SITE_DIR, 'data')

# Reuse the country-keywords loader so subject_merges + correction filtering match
sys.path.insert(0, os.path.abspath(os.path.join(SITE_DIR, '..', 'projects',
                                                '2026-05-top-keyword',
                                                '2026-05-13-world-country-tweets',
                                                'scripts')))
import build_country_keywords as bck

CORR_RE = re.compile(r'/(c-)?corrections?-|/pageoneplus/corrections-')
def is_corr(a):
    sb = a.get('sb') or []
    if 'Correction Stories' in sb: return True
    if (a.get('s') or '') == 'Corrections': return True
    return bool(CORR_RE.search(a.get('u') or ''))

MIN_COUNTRY_ARTICLES = 15      # include small countries so they appear in the picker
MIN_TAG_COUNT_PER_COUNTRY = 3  # don't carry per-country tag entries thinner than this


def main():
    print('Loading articles + applying subject merges…')
    arts = bck.load_world_articles()
    world = [a for a in arts if (a.get('s') or '') == 'World']
    world_clean = [a for a in world if not is_corr(a)]
    print(f'  World section: {len(world):,}  (clean: {len(world_clean):,})')

    print('Computing corpus baseline (with per-year breakdowns)…')
    corpus_freq = Counter()
    corpus_freq_by_year = defaultdict(Counter)   # tag → year → count
    corpus_total_by_year = Counter()
    for a in world_clean:
        yr = (a.get('d') or '')[:4]
        if yr:
            corpus_total_by_year[yr] += 1
        for tag in set(a.get('sb') or []):
            corpus_freq[tag] += 1
            if yr:
                corpus_freq_by_year[tag][yr] += 1
    corpus_total = len(world_clean)

    print('Tabulating per-country tags…')
    # Bodies of water, regions, and other non-country geographic tags that
    # appear in NYT's location vocabulary. The themes view is a per-country
    # tool, so these don't belong in the country picker.
    SKIP_LOCATIONS = {
        # Continents / regional aggregates
        'Africa', 'Europe', 'Asia', 'Latin America', 'Caribbean Area',
        'Middle East', 'North America', 'South America', 'Central America',
        'West Africa', 'East Africa', 'North Africa', 'Southern Africa',
        'Central Asia', 'Southeast Asia', 'East Asia', 'South Asia',
        'Eastern Europe', 'Western Europe', 'Central Europe', 'Northern Europe',
        'Southern Europe', 'Scandinavia', 'Sub-Saharan Africa',
        'Far East, South and Southeast Asia and Pacific Areas',
        # Bodies of water
        'Arctic Ocean', 'Pacific Ocean', 'Atlantic Ocean', 'Indian Ocean',
        'Mediterranean Sea', 'Caribbean Sea', 'Black Sea', 'Baltic Sea',
        'Red Sea', 'South China Sea', 'East China Sea', 'Aegean Sea',
        'Barents Sea', 'Persian Gulf', 'Gulf of Aden', 'Gulf of Mexico',
        'Gulf of Oman', 'Gulf of Thailand', 'Strait of Hormuz', 'English Channel',
        # Rivers / deserts / mountains
        'Sahara Desert', 'Gobi Desert', 'Amazon River Basin', 'Amazon River',
        'Ganges River', 'Mekong River', 'Danube River', 'Nile River',
        'Tigris River', 'Euphrates River', 'Alps Mountains', 'Andes Mountains',
        'Himalayan Mountains', 'Rocky Mountains', 'Pyrenees Mountains',
        # More water bodies (sub-30 entries)
        'Caspian Sea', 'North Sea', 'Arabian Sea', 'Bay of Bengal',
        'Kerch Strait', 'Taiwan Strait', 'Strait of Malacca',
        # Sub-national / contested / regional
        'Baltic Region', 'Horn of Africa', 'Kurdistan', 'Abu Ghraib',
        # Polar — keep Antarctic Regions as a place (NYT uses it as the
        # canonical Antarctica tag). Arctic Regions stays skipped: it's a
        # region spanning multiple sovereign territories.
        'Arctic Regions',
        # Contested islands often tagged without a parent country
        'Spratly Islands', 'Paracel Islands', 'Senkaku Islands',
    }

    country_total = Counter()
    country_total_by_year = defaultdict(Counter)
    country_tag_counts = defaultdict(Counter)
    country_tag_years = defaultdict(lambda: defaultdict(Counter))
    for a in world_clean:
        locs = a.get('gn') or a.get('g') or []
        locs = [l for l in locs if l not in SKIP_LOCATIONS]
        yr = (a.get('d') or '')[:4]
        seen = set()
        for c in locs:
            country_total[c] += 1
            if yr:
                country_total_by_year[c][yr] += 1
            for tag in (a.get('sb') or []):
                if tag in seen: continue
                country_tag_counts[c][tag] += 1
                if yr:
                    country_tag_years[c][tag][yr] += 1
            for tag in (a.get('sb') or []):
                seen.add(tag)

    print(f'  Countries tracked: {len(country_total)}')

    # Compose output, only include countries with enough coverage
    countries = {}
    for c, n in country_total.items():
        if n < MIN_COUNTRY_ARTICLES:
            continue
        tags = {}
        for tag, cnt in country_tag_counts[c].items():
            if cnt < MIN_TAG_COUNT_PER_COUNTRY:
                continue
            tags[tag] = {
                'count': cnt,
                'years': dict(country_tag_years[c][tag]),
            }
        countries[c] = {
            'total': n,
            'total_by_year': dict(country_total_by_year[c]),
            'tags': tags,
        }

    print(f'  Countries exported: {len(countries)}')

    # Load tag_config and surface the exclusion bits we'll let the UI toggle
    with open(os.path.join(DATA_DIR, 'tag_config.json')) as f:
        cfg = json.load(f)
    filters = {
        'world_generic_subjects':   cfg.get('world_coverage_generic_subjects', []),
        'world_generic_substrings': cfg.get('world_coverage_generic_substrings', []),
        'world_generic_prefixes':   list(cfg.get('generic_prefixes_always_filter', [])) + ['Content Type:', 'Content type:', 'Vis-'],
        'generic_always':           cfg.get('generic_subjects_always_filter', []),
        'headline_event_tags':      cfg.get('headline_event_tags', []),
        'headline_event_patterns':  cfg.get('headline_event_patterns', []),
        'country_exclusion_tags':   cfg.get('country_exclusion_tags', {}),
        'country_event_tags':       cfg.get('country_event_tags', {}),
    }

    # Country silhouettes (simplified to keep file small)
    print('Embedding country geometries…')
    import geopandas as gpd
    gdf = gpd.read_file(os.path.join(DATA_DIR, 'world_countries.geojson'))
    name_field = 'NAME' if 'NAME' in gdf.columns else 'name'
    # geojson NAME → analysis-side name (subset for the regions we render)
    geojson_to_analysis = {
        'United Kingdom': 'Great Britain',
        'Czechia': 'Czech Republic',
        'Bosnia and Herz.': 'Bosnia and Herzegovina',
        'Dem. Rep. Congo': 'Democratic Republic of Congo',
        'Central African Rep.': 'Central African Republic',
        "Côte d'Ivoire": 'Ivory Coast',
        'Eq. Guinea': 'Equatorial Guinea',
        'S. Sudan': 'South Sudan',
        'eSwatini': 'Eswatini',
        'W. Sahara': 'Western Sahara',
        'Congo': 'Republic of Congo',
        'Antarctica': 'Antarctic Regions',
    }

    def simplify_geom(geom, tolerance=0.05):
        try:
            g = geom.simplify(tolerance, preserve_topology=True)
        except Exception:
            g = geom
        raw_polys = list(g.geoms) if g.geom_type == 'MultiPolygon' else [g]
        raw_polys = [p for p in raw_polys if p.exterior]
        if not raw_polys:
            return []
        # Drop far-flung overseas territories: keep only polygons whose
        # centroid sits within ~25° of the largest polygon's centroid.
        # Mirrors the rule in build_country_cards.py so France/Netherlands/etc.
        # don't drag overseas departments into the silhouette.
        if len(raw_polys) > 1:
            main = max(raw_polys, key=lambda p: p.area)
            mx0, my0, mx1, my1 = main.bounds
            mcx, mcy = (mx0+mx1)/2, (my0+my1)/2
            kept = []
            for p in raw_polys:
                px0, py0, px1, py1 = p.bounds
                pcx, pcy = (px0+px1)/2, (py0+py1)/2
                if abs(pcx-mcx) <= 25 and abs(pcy-mcy) <= 25:
                    kept.append(p)
            if kept:
                raw_polys = kept
        return [[[round(x, 3), round(y, 3)] for x, y in p.exterior.coords]
                for p in raw_polys]

    country_geometry = {}
    for _, row in gdf.iterrows():
        nm = row[name_field]
        analysis = geojson_to_analysis.get(nm, nm)
        if analysis in countries:
            country_geometry[analysis] = simplify_geom(row.geometry)

    # Natural Earth 110m omits Bahrain (too small). Provide a synthetic
    # silhouette of the main island.
    if 'Bahrain' in countries:
        country_geometry['Bahrain'] = [[
            [50.45, 26.30], [50.65, 26.28], [50.70, 26.20],
            [50.65, 25.80], [50.48, 25.79], [50.42, 25.95],
            [50.45, 26.30],
        ]]

    # Only emit corpus_freq_by_year for tags we actually use (any country tag).
    used_tags = set()
    for c in countries.values():
        used_tags.update(c['tags'].keys())
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
        'countries': countries,
        'filters': filters,
        'country_geometry': country_geometry,
    }

    out_path = os.path.join(DATA_DIR, 'themes_explorer.json')
    with open(out_path, 'w') as f:
        # Compact JSON to keep file size down
        json.dump(out, f, separators=(',', ':'))
    size = os.path.getsize(out_path) / (1024 * 1024)
    print(f'Saved {out_path} ({size:.1f} MB)')


if __name__ == '__main__':
    main()
