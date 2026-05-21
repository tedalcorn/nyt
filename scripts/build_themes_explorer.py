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

    # Location merges — variant tags that should fold into one canonical name
    # (parallel to subject_merges but for glocations). Mirrored in index.html
    # so the world tab and themes tab show the same thing.
    LOCATION_MERGES = {
        'Antarctic Regions': 'Antarctica',
        'Macedonia': 'North Macedonia',
    }

    country_total = Counter()
    country_total_by_year = defaultdict(Counter)
    country_tag_counts = defaultdict(Counter)
    country_tag_years = defaultdict(lambda: defaultdict(Counter))
    for a in world_clean:
        locs = a.get('gn') or a.get('g') or []
        locs = [LOCATION_MERGES.get(l, l) for l in locs if l not in SKIP_LOCATIONS]
        yr = (a.get('d') or '')[:4]
        unique_tags = set(a.get('sb') or [])
        for c in set(locs):
            country_total[c] += 1
            if yr:
                country_total_by_year[c][yr] += 1
            for tag in unique_tags:
                country_tag_counts[c][tag] += 1
                if yr:
                    country_tag_years[c][tag][yr] += 1

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
    }

    # Extract Crimea + Sevastopol polygons from Russia's MultiPolygon — they'll
    # be appended to Ukraine's silhouette below (Natural Earth tags Crimea as
    # Russia, reflecting de-facto control post-2014; internationally it's
    # Ukraine). Pulling the polygons aside here lets the regular country loop
    # build the silhouettes in one pass without needing a shapely union (which
    # breaks under numpy 2 / shapely 2.0).
    crimea_polys_for_ukraine = []
    ru_idx = gdf.index[gdf[name_field] == 'Russia']
    if len(ru_idx):
        ru_geom = gdf.loc[ru_idx[0], 'geometry']
        ru_polys = list(ru_geom.geoms) if ru_geom.geom_type == 'MultiPolygon' else [ru_geom]
        for p in ru_polys:
            # Crimea piece in NE 110m sits roughly 32.5-36.5°E, 44.4-46.2°N,
            # detached from mainland Russia (which spans to 180°E).
            x0, y0, x1, y1 = p.bounds
            if 32 <= x0 and x1 <= 37 and 44 <= y0 and y1 <= 47:
                crimea_polys_for_ukraine.append(p)
        if crimea_polys_for_ukraine:
            print(f'  Reassigning {len(crimea_polys_for_ukraine)} Crimea polygon(s) Russia→Ukraine')

    def simplify_geom(geom, tolerance=0.05, extra_polys=(), skip_polys=()):
        """Simplify a country's geometry and return [[[x,y], ...], ...].

        `extra_polys` — additional polygons to include before simplification
        (used to graft Crimea into Ukraine's silhouette).
        `skip_polys` — polygons to drop from the source (used to remove Crimea
        from Russia's silhouette, identified by exact bounds match).
        """
        try:
            g = geom.simplify(tolerance, preserve_topology=True)
        except Exception:
            g = geom
        raw_polys = list(g.geoms) if g.geom_type == 'MultiPolygon' else [g]
        raw_polys = [p for p in raw_polys if p.exterior]
        if skip_polys:
            skip_bounds = {p.bounds for p in skip_polys}
            raw_polys = [p for p in raw_polys if p.bounds not in skip_bounds]
        for ep in extra_polys:
            try:
                raw_polys.append(ep.simplify(tolerance, preserve_topology=True))
            except Exception:
                raw_polys.append(ep)
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
        if analysis not in countries:
            continue
        if nm == 'Ukraine':
            country_geometry[analysis] = simplify_geom(
                row.geometry, extra_polys=crimea_polys_for_ukraine)
        elif nm == 'Russia':
            country_geometry[analysis] = simplify_geom(
                row.geometry, skip_polys=crimea_polys_for_ukraine)
        else:
            country_geometry[analysis] = simplify_geom(row.geometry)

    # Natural Earth 110m omits Bahrain (too small). Provide a synthetic
    # silhouette of the main island.
    if 'Bahrain' in countries:
        country_geometry['Bahrain'] = [[
            [50.45, 26.30], [50.65, 26.28], [50.70, 26.20],
            [50.65, 25.80], [50.48, 25.79], [50.42, 25.95],
            [50.45, 26.30],
        ]]

    # Antarctica: use the higher-resolution Natural Earth 50m polygon
    # (Natural Earth 110m has only 14 vertices for the main continent).
    # Project to South Polar Stereographic (EPSG:3031) so it renders as a
    # recognizable continent rather than a flat lat/lon strip.
    if 'Antarctica' in countries:
        try:
            ant_path = os.path.join(DATA_DIR, 'antarctica_50m.geojson')
            if os.path.exists(ant_path):
                ant_gdf = gpd.read_file(ant_path)
                projected = ant_gdf.to_crs('EPSG:3031').iloc[0].geometry
                projected = projected.simplify(8000, preserve_topology=True)
                polys_p = list(projected.geoms) if projected.geom_type == 'MultiPolygon' else [projected]
                # Keep the main continent + any sub-polygon ≥ 5% of the largest
                main_area = max(p.area for p in polys_p)
                kept = [p for p in polys_p if p.area >= 0.05 * main_area]
                country_geometry['Antarctica'] = [
                    [[round(x/1000, 1), round(y/1000, 1)] for x, y in p.exterior.coords]
                    for p in kept
                ]
        except Exception as e:
            print(f'  (could not project Antarctica: {e})')

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
