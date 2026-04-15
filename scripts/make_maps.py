"""
Generate annual choropleth maps of NYT article coverage.
Outputs PNGs to maps/world/ and maps/us/ subdirectories.
"""

import json, os, zipfile, urllib.request
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
from collections import defaultdict
from shapely.affinity import scale as shp_scale, translate as shp_translate

# ── Config ───────────────────────────────────────────────────────────────────
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR    = os.path.join(PROJECT_DIR, 'data')
OUTPUT_DIR  = PROJECT_DIR
YEARS       = [str(y) for y in range(2000, 2026)]

FIG_W,    FIG_H    = 14, 8   # world
FIG_US_W, FIG_US_H = 13, 8  # US

BG_COLOR = '#f8f4ef'   # warm off-white
NO_DATA  = '#d8d0c8'   # muted tan for no-data
CMAP_NAME = 'Blues'

# ── NYT location name → Natural Earth ADMIN name ─────────────────────────────
WORLD_NAME_MAP = {
    'Great Britain':                    'United Kingdom',
    'England':                          'United Kingdom',
    'Scotland':                         'United Kingdom',
    'Wales':                            'United Kingdom',
    'Northern Ireland':                 'United Kingdom',
    'United States':                    'United States of America',
    'South Korea':                      'South Korea',
    'North Korea':                      'North Korea',
    'Democratic Republic of Congo':     'Dem. Rep. Congo',
    'Democratic Republic of the Congo': 'Dem. Rep. Congo',
    'Republic of Congo':                'Congo',
    'Congo (Brazzaville)':              'Congo',
    'Congo (Kinshasa)':                 'Dem. Rep. Congo',
    'Ivory Coast':                      "Côte d'Ivoire",
    "Cote d'Ivoire":                    "Côte d'Ivoire",
    'Czech Republic':                   'Czechia',
    'Serbia and Montenegro':            'Serbia',
    'Yugoslavia':                       'Serbia',
    'East Timor':                       'Timor-Leste',
    'Burma':                            'Myanmar',
    'Swaziland':                        'Eswatini',
    'Macedonia':                        'North Macedonia',
    'Slovak Republic':                  'Slovakia',
    'Trinidad and Tobago':              'Trinidad and Tobago',
    'Bosnia and Herzegovina':           'Bosnia and Herz.',
    'Bosnia-Herzegovina':               'Bosnia and Herz.',
    'Central African Republic':         'Central African Rep.',
    'Solomon Islands':                  'Solomon Is.',
    'Equatorial Guinea':                'Eq. Guinea',
    'South Sudan':                      'S. Sudan',
    'Palestinian Authority':            'Palestine',
    'Palestinian Territories':          'Palestine',
    'West Bank':                        'Palestine',
    'Gaza Strip':                       'Palestine',
    'Kyrgyzstan':                       'Kyrgyzstan',
    'Cape Verde':                       'Cabo Verde',
    'Laos':                             'Laos',
    'Vatican City':                     'Vatican',
    'Micronesia':                       'Micronesia',
    'Netherlands Antilles':             'Netherlands',
    'The Gambia':                       'Gambia',
    'United Arab Emirates':             'United Arab Emirates',
}

# ── Download / cache world shapefile ─────────────────────────────────────────
SHAPE_DIR = '/tmp/ne_world'
SHAPE_ZIP = '/tmp/ne_world.zip'
SHAPE_URL = 'https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip'

def get_world_gdf():
    if not os.path.exists(SHAPE_DIR):
        if not os.path.exists(SHAPE_ZIP):
            print('Downloading Natural Earth world shapefile...')
            urllib.request.urlretrieve(SHAPE_URL, SHAPE_ZIP)
        with zipfile.ZipFile(SHAPE_ZIP) as z:
            z.extractall(SHAPE_DIR)
    return gpd.read_file(SHAPE_DIR)

# ── Per-year article counts ───────────────────────────────────────────────────
def load_dashboard():
    with open(os.path.join(DATA_DIR, 'dashboard.json')) as f:
        return json.load(f)

def world_counts_by_year(dashboard):
    """Returns {year: {NE_admin_name: count}} — US excluded (greyed out)."""
    lt = dashboard['world_coverage']['location_trends']
    by_year = defaultdict(dict)
    for loc, year_dict in lt.items():
        mapped = WORLD_NAME_MAP.get(loc, loc)
        if mapped == 'United States of America':
            continue   # US tracked separately; keep it grey on world map
        for yr, cnt in year_dict.items():
            by_year[yr][mapped] = by_year[yr].get(mapped, 0) + cnt
    return by_year

def state_counts_by_year(dashboard):
    """Returns {year: {state_name: count}} merging U.S. and New York sections."""
    usc = dashboard['us_state_coverage']
    by_year = defaultdict(lambda: defaultdict(int))
    # U.S. section
    for state, yd in usc['state_trends'].items():
        for yr, cnt in yd.items():
            by_year[yr][state] += cnt
    # New York section
    for state, yd in usc.get('ny_state_trends', {}).items():
        for yr, cnt in yd.items():
            by_year[yr][state] += cnt
    return {yr: dict(counts) for yr, counts in by_year.items()}

# ── Norm helpers ──────────────────────────────────────────────────────────────
def make_log_norm(all_counts, pct_cap=97):
    vals = [v for d in all_counts.values() for v in d.values() if v > 0]
    if not vals:
        return mcolors.LogNorm(vmin=1, vmax=10)
    vmax = np.percentile(vals, pct_cap)
    return mcolors.LogNorm(vmin=1, vmax=max(vmax, 2))

def make_linear_norm(all_counts, pct_cap=97):
    vals = [v for d in all_counts.values() for v in d.values() if v > 0]
    if not vals:
        return mcolors.Normalize(vmin=0, vmax=100)
    vmax = np.percentile(vals, pct_cap)
    return mcolors.Normalize(vmin=0, vmax=max(vmax, 1))

# ── AK / HI inset helper ──────────────────────────────────────────────────────
def _fit_geom(geom, target_minx, target_miny, target_maxx, target_maxy):
    """Scale geometry proportionally and translate to fill a target bbox."""
    b = geom.bounds
    sw, sh = b[2]-b[0], b[3]-b[1]
    tw, th = target_maxx-target_minx, target_maxy-target_miny
    sf = min(tw/sw, th/sh) if sw > 0 and sh > 0 else 1.0
    g2 = shp_scale(geom, xfact=sf, yfact=sf, origin=(b[0], b[1]))
    b2 = g2.bounds
    return shp_translate(g2, xoff=target_minx-b2[0], yoff=target_miny-b2[1])

def build_us_gdf(gdf_raw, name_col):
    """
    Return a GeoDataFrame in EPSG:5070 with Alaska and Hawaii repositioned
    as small insets in the lower-left corner.
    Alaska is projected via EPSG:3338 (Alaska Albers) before rescaling to
    avoid the severe distortion caused by the Aleutian Islands in EPSG:5070.
    Hawaii is projected via EPSG:32604 (UTM zone 4N).
    """
    INSET_STATES = {'Alaska', 'Hawaii'}
    cont = gdf_raw[~gdf_raw[name_col].isin(INSET_STATES)].to_crs('EPSG:5070').copy()
    cb = cont.total_bounds          # [minx, miny, maxx, maxy]
    cw, ch = cb[2]-cb[0], cb[3]-cb[1]

    extra_rows = []

    # ── Alaska ──
    ak_rows = gdf_raw[gdf_raw[name_col] == 'Alaska']
    if len(ak_rows):
        ak_geom = ak_rows.to_crs('EPSG:3338').geometry.iloc[0]
        # Place in lower-left: 28 % wide × 20 % tall of continental extent
        ak_fitted = _fit_geom(ak_geom,
                              cb[0],              cb[1],
                              cb[0] + cw * 0.28,  cb[1] + ch * 0.20)
        r = ak_rows.to_crs('EPSG:5070').iloc[0].to_dict()
        r['geometry'] = ak_fitted
        extra_rows.append(r)

    # ── Hawaii ──
    hi_rows = gdf_raw[gdf_raw[name_col] == 'Hawaii']
    if len(hi_rows):
        hi_geom = hi_rows.to_crs('EPSG:32604').geometry.iloc[0]
        # Place immediately right of AK inset: 13 % wide × 8 % tall
        hi_fitted = _fit_geom(hi_geom,
                              cb[0] + cw * 0.30,  cb[1],
                              cb[0] + cw * 0.43,  cb[1] + ch * 0.08)
        r = hi_rows.to_crs('EPSG:5070').iloc[0].to_dict()
        r['geometry'] = hi_fitted
        extra_rows.append(r)

    if extra_rows:
        extra_gdf = gpd.GeoDataFrame(extra_rows, crs='EPSG:5070')
        cont = pd.concat([cont, extra_gdf], ignore_index=True)

    return cont

# ── World maps ────────────────────────────────────────────────────────────────
def make_world_maps(dashboard, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    gdf = get_world_gdf()
    gdf = gdf[gdf['CONTINENT'] != 'Antarctica'].copy()
    gdf = gdf.to_crs('ESRI:54030')   # Robinson projection

    counts = world_counts_by_year(dashboard)
    norm   = make_log_norm(counts)
    cmap   = plt.get_cmap(CMAP_NAME)

    for year in YEARS:
        year_counts = counts.get(year, {})
        gdf['count'] = gdf['ADMIN'].map(year_counts).fillna(0)

        fig, ax = plt.subplots(1, 1, figsize=(FIG_W, FIG_H))
        fig.patch.set_facecolor(BG_COLOR)
        ax.set_facecolor(BG_COLOR)

        # No-data countries (including US — not part of world coverage)
        no_data  = gdf[gdf['count'] == 0]
        no_data.plot(ax=ax, color=NO_DATA, linewidth=0.3, edgecolor='white')

        has_data = gdf[gdf['count'] > 0].copy()
        if not has_data.empty:
            has_data.plot(ax=ax, column='count', cmap=cmap, norm=norm,
                          linewidth=0.3, edgecolor='white')

        ax.set_axis_off()
        ax.set_xlim(gdf.total_bounds[[0, 2]])
        ax.set_ylim(gdf.total_bounds[[1, 3]])

        # Title: two-line, centered above map
        ax.text(0.5, 1.01,
                'World Coverage in the New York Times',
                transform=ax.transAxes, ha='center', va='bottom',
                fontsize=13, color='#333333', fontfamily='serif')
        ax.text(0.5, 1.065,
                year,
                transform=ax.transAxes, ha='center', va='bottom',
                fontsize=22, fontweight='bold', color='#1a1a1a', fontfamily='serif')

        # Colorbar
        sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
        sm.set_array([])
        cbar = fig.colorbar(sm, ax=ax, orientation='horizontal',
                            fraction=0.025, pad=0.02, aspect=40)
        cbar.set_label('Articles mentioning country (World section)', fontsize=10, color='#444')
        cbar.ax.tick_params(labelsize=8, colors='#444')

        plt.tight_layout(pad=0.5)
        out_path = os.path.join(out_dir, f'world_{year}.png')
        fig.savefig(out_path, dpi=150, bbox_inches='tight',
                    facecolor=BG_COLOR, edgecolor='none')
        plt.close(fig)
        print(f'  Saved {out_path}')

# ── US state maps ─────────────────────────────────────────────────────────────
def make_us_maps(dashboard, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    gdf_raw = gpd.read_file(os.path.join(DATA_DIR, 'us_states.geojson'))

    # Identify name column
    name_col = next((c for c in ['name','NAME','state','STATE','State']
                     if c in gdf_raw.columns), None)
    if name_col is None:
        raise ValueError(f'Cannot find state name column in: {gdf_raw.columns.tolist()}')

    # Build repositioned GDF once (AK/HI as lower-left insets)
    gdf_proj = build_us_gdf(gdf_raw, name_col)

    counts = state_counts_by_year(dashboard)
    norm   = make_linear_norm(counts)
    cmap   = plt.get_cmap(CMAP_NAME)

    for year in YEARS:
        year_counts = counts.get(year, {})
        gdf_year = gdf_proj.copy()
        gdf_year['count'] = gdf_year[name_col].map(year_counts).fillna(0)

        fig, ax = plt.subplots(1, 1, figsize=(FIG_US_W, FIG_US_H))
        fig.patch.set_facecolor(BG_COLOR)
        ax.set_facecolor(BG_COLOR)

        no_data  = gdf_year[gdf_year['count'] == 0]
        no_data.plot(ax=ax, color=NO_DATA, linewidth=0.5, edgecolor='white')

        has_data = gdf_year[gdf_year['count'] > 0]
        if not has_data.empty:
            has_data.plot(ax=ax, column='count', cmap=cmap, norm=norm,
                          linewidth=0.5, edgecolor='white')

        ax.set_axis_off()

        # Title: two-line, centered
        ax.text(0.5, 1.01,
                'U.S. Coverage in the New York Times',
                transform=ax.transAxes, ha='center', va='bottom',
                fontsize=13, color='#333333', fontfamily='serif')
        ax.text(0.5, 1.065,
                year,
                transform=ax.transAxes, ha='center', va='bottom',
                fontsize=22, fontweight='bold', color='#1a1a1a', fontfamily='serif')

        # Colorbar
        sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
        sm.set_array([])
        cbar = fig.colorbar(sm, ax=ax, orientation='horizontal',
                            fraction=0.025, pad=0.02, aspect=40)
        cbar.set_label('Articles mentioning state (U.S. + New York sections)',
                       fontsize=10, color='#444')
        cbar.ax.tick_params(labelsize=8, colors='#444')

        plt.tight_layout(pad=0.5)
        out_path = os.path.join(out_dir, f'us_{year}.png')
        fig.savefig(out_path, dpi=150, bbox_inches='tight',
                    facecolor=BG_COLOR, edgecolor='none')
        plt.close(fig)
        print(f'  Saved {out_path}')

# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print('Loading dashboard data...')
    dashboard = load_dashboard()

    print(f'\nGenerating world maps ({len(YEARS)} years)...')
    make_world_maps(dashboard, os.path.join(OUTPUT_DIR, 'maps', 'world'))

    print(f'\nGenerating U.S. state maps ({len(YEARS)} years)...')
    make_us_maps(dashboard, os.path.join(OUTPUT_DIR, 'maps', 'us'))

    print('\nDone.')
