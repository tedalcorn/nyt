"""
Generate annual choropleth maps of NYT article coverage.
Outputs PNGs to maps/world/ and maps/us/ subdirectories.
"""

import json, os, zipfile, urllib.request
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.patches as mpatches
import numpy as np
from collections import defaultdict
from shapely.affinity import scale as shp_scale, translate as shp_translate
from matplotlib.colors import PowerNorm, ListedColormap

# Muted blues matching the website's US state choropleth (Q1=lightest → Q5=darkest)
_US_Q_PALETTE = ListedColormap(['#dce9f5', '#9dbdda', '#5e96c0', '#2c6fa3', '#0d3d6b'])

# ── Config ───────────────────────────────────────────────────────────────────
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR    = os.path.join(PROJECT_DIR, 'data')
OUTPUT_DIR  = PROJECT_DIR
YEARS       = [str(y) for y in range(2000, 2026)]

FIG_W,    FIG_H    = 14, 8   # world (landscape)
FIG_US_W, FIG_US_H = 13, 8  # US (landscape)

# Instagram portrait: 1080×1350 px at 150 dpi = 7.2 × 9.0 inches
FIG_IG_W, FIG_IG_H = 7.2, 9.0
IG_MAP_RECT   = [0.01, 0.38, 0.98, 0.62]   # map from 38%→100%; bottom 38% for captions
IG_CBAR_RECT  = [0.10, 0.44, 0.80, 0.024]  # colorbar overlaid ~44% from bottom
IG_TITLE_Y    = 0.970   # title/subtitle text (on top, larger)
IG_SUBTITLE_Y = 0.930   # year number (below title, smaller)

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

# ── U.S. state populations (2020 Census, millions) ───────────────────────────
STATE_POP_M = {
    'Alabama': 5.024, 'Alaska': 0.733, 'Arizona': 7.151, 'Arkansas': 3.011,
    'California': 39.538, 'Colorado': 5.774, 'Connecticut': 3.606, 'Delaware': 0.989,
    'Florida': 21.538, 'Georgia': 10.712, 'Hawaii': 1.455, 'Idaho': 1.839,
    'Illinois': 12.812, 'Indiana': 6.786, 'Iowa': 3.190, 'Kansas': 2.938,
    'Kentucky': 4.506, 'Louisiana': 4.658, 'Maine': 1.362, 'Maryland': 6.178,
    'Massachusetts': 7.030, 'Michigan': 10.077, 'Minnesota': 5.707, 'Mississippi': 2.961,
    'Missouri': 6.155, 'Montana': 1.085, 'Nebraska': 1.962, 'Nevada': 3.104,
    'New Hampshire': 1.378, 'New Jersey': 9.289, 'New Mexico': 2.117, 'New York': 20.201,
    'North Carolina': 10.440, 'North Dakota': 0.779, 'Ohio': 11.800, 'Oklahoma': 3.959,
    'Oregon': 4.238, 'Pennsylvania': 13.002, 'Rhode Island': 1.098, 'South Carolina': 5.119,
    'South Dakota': 0.887, 'Tennessee': 6.910, 'Texas': 29.146, 'Utah': 3.272,
    'Vermont': 0.644, 'Virginia': 8.632, 'Washington': 7.706, 'West Virginia': 1.794,
    'Wisconsin': 5.894, 'Wyoming': 0.577, 'District of Columbia': 0.689,
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

def make_quintile_norm(all_counts):
    """5-bucket discrete norm (quintiles across all years) — stable coloring over time."""
    vals = sorted(v for d in all_counts.values() for v in d.values() if v > 0)
    if not vals:
        return mcolors.BoundaryNorm([0, 1, 5, 20, 100, 500], 5), 5
    bounds = [0.0] + [float(np.percentile(vals, p)) for p in [20, 40, 60, 80, 100]]
    # Ensure strictly increasing
    clean = [bounds[0]]
    for b in bounds[1:]:
        clean.append(max(b, clean[-1] + 0.5))
    n = len(clean) - 1
    return mcolors.BoundaryNorm(clean, n), n

def us_permillion_counts(counts):
    """Normalize state article counts to articles per million residents."""
    out = {}
    for yr, state_dict in counts.items():
        out[yr] = {}
        for state, cnt in state_dict.items():
            pop = STATE_POP_M.get(state)
            if pop:
                out[yr][state] = cnt / pop
    return out

def _us_view_limits(gdf_proj, ax_w_in, ax_h_in, pad=1.015, x_shift_frac=0.0):
    """
    Compute xlim/ylim so the US fills the axes with equal physical scale.
    x_shift_frac > 0 shifts the view left, moving map content right on screen.
    """
    b = gdf_proj.total_bounds
    dw = b[2] - b[0]; dh = b[3] - b[1]
    cx = (b[0] + b[2]) / 2; cy = (b[1] + b[3]) / 2
    ax_ratio = ax_w_in / ax_h_in
    if dw / dh < ax_ratio:
        vh = dh * pad; vw = vh * ax_ratio
    else:
        vw = dw * pad; vh = vw / ax_ratio
    shift = x_shift_frac * vw
    return (cx - vw/2 - shift, cx + vw/2 - shift), (cy - vh/2, cy + vh/2)

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

# ── Shared landscape layout constants ─────────────────────────────────────────
# Bottom 15% of figure is blank (reserved for captions); map fills 15–100%.
# Title and colorbar OVERLAY on the map (drawn after it, zorder > map).
_L_MAP_RECT  = [0.005, 0.15, 0.990, 0.85]   # map: 15%–100% of figure height
_L_CBAR_RECT = [0.12,  0.30, 0.76,  0.022]  # colorbar overlaid at 30% from bottom
_L_TITLE_Y   = 0.972   # descriptive title (va='bottom') overlaid at top
_L_YEAR_Y    = 0.940   # year label (va='bottom') just below title

def _add_overlay_bg(fig, rect, pad_x=0.005, pad_y=0.010, alpha=0.80):
    """Add a semi-transparent background patch behind a colorbar or legend overlay."""
    p = mpatches.FancyBboxPatch(
        (rect[0] - pad_x, rect[1] - pad_y),
        rect[2] + 2*pad_x, rect[3] + 2*pad_y,
        boxstyle='round,pad=0', linewidth=0,
        facecolor='#f8f4ef', edgecolor='none', alpha=alpha,
        transform=fig.transFigure, zorder=4
    )
    fig.add_artist(p)

# ── World maps ────────────────────────────────────────────────────────────────
def make_world_maps(dashboard, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    gdf = get_world_gdf()
    gdf = gdf[gdf['CONTINENT'] != 'Antarctica'].copy()
    gdf = gdf.to_crs('ESRI:54030')   # Robinson projection
    tb  = gdf.total_bounds            # compute once

    counts = world_counts_by_year(dashboard)
    norm   = make_log_norm(counts)
    cmap   = plt.get_cmap(CMAP_NAME)

    for year in YEARS:
        year_counts = counts.get(year, {})
        gdf['count'] = gdf['ADMIN'].map(year_counts).fillna(0)

        fig = plt.figure(figsize=(FIG_W, FIG_H))
        fig.patch.set_facecolor(BG_COLOR)
        ax = fig.add_axes(_L_MAP_RECT)
        ax.set_facecolor(BG_COLOR)

        gdf[gdf['count'] == 0].plot(ax=ax, color=NO_DATA, linewidth=0.3, edgecolor='white')
        has_data = gdf[gdf['count'] > 0].copy()
        if not has_data.empty:
            has_data.plot(ax=ax, column='count', cmap=cmap, norm=norm,
                          linewidth=0.3, edgecolor='white')

        ax.set_axis_off()
        yrange = tb[3] - tb[1]
        ax.set_xlim(tb[0], tb[2])
        ax.set_ylim(tb[1] + yrange * 0.01, tb[3] - yrange * 0.05)

        # Titles overlaid at top — semi-transparent bg for legibility
        _add_overlay_bg(fig, [0.05, _L_YEAR_Y - 0.005, 0.90, 0.065], pad_y=0.005, alpha=0.75)
        fig.text(0.5, _L_TITLE_Y, 'World Coverage in the New York Times',
                 ha='center', va='bottom', fontsize=17, color='#333333', fontfamily='serif',
                 zorder=6)
        fig.text(0.5, _L_YEAR_Y, year,
                 ha='center', va='bottom', fontsize=14, fontweight='bold',
                 color='#1a1a1a', fontfamily='serif', zorder=6)

        # Colorbar overlaid ~27% up — background patch then bar
        _add_overlay_bg(fig, _L_CBAR_RECT, pad_x=0.015, pad_y=0.014, alpha=0.80)
        cbar_ax = fig.add_axes(_L_CBAR_RECT, zorder=5)
        sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
        sm.set_array([])
        cbar = fig.colorbar(sm, cax=cbar_ax, orientation='horizontal')
        cbar.set_label('Articles mentioning country (World section)', fontsize=9, color='#444')
        cbar.ax.tick_params(labelsize=7.5, colors='#444')

        out_path = os.path.join(out_dir, f'world_{year}.png')
        fig.savefig(out_path, dpi=150, facecolor=BG_COLOR, edgecolor='none')
        plt.close(fig)
        print(f'  Saved {out_path}')

# ── US quintile helpers ───────────────────────────────────────────────────────
# Fixed colormap: 5 discrete blues for quintile ranks 1 (least) → 5 (most)
_US_Q_CMAP = None   # initialised lazily after plt is available
_US_Q_NORM = mcolors.BoundaryNorm([0.5, 1.5, 2.5, 3.5, 4.5, 5.5], 5)

def _year_quintile_col(gdf, name_col, year_raw_counts):
    """
    Return a Series of within-year quintile ranks (1–5) aligned to gdf.
    Rank is based on articles-per-million for that year only.
    States with 0 articles get 0 (rendered as NO_DATA).
    """
    perm = {}
    for state, cnt in year_raw_counts.items():
        pop = STATE_POP_M.get(state)
        if pop and cnt > 0:
            perm[state] = cnt / pop
    if not perm:
        return pd.Series(0, index=gdf.index)
    ranked = sorted(perm.items(), key=lambda x: x[1])
    n = len(ranked)
    qmap = {state: min(5, int(i * 5 / n) + 1) for i, (state, _) in enumerate(ranked)}
    return gdf[name_col].map(qmap).fillna(0)

# ── US state maps ─────────────────────────────────────────────────────────────
def make_us_maps(dashboard, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    gdf_raw = gpd.read_file(os.path.join(DATA_DIR, 'us_states.geojson'))

    name_col = next((c for c in ['name','NAME','state','STATE','State']
                     if c in gdf_raw.columns), None)
    if name_col is None:
        raise ValueError(f'Cannot find state name column in: {gdf_raw.columns.tolist()}')

    gdf_proj = build_us_gdf(gdf_raw, name_col)
    counts_raw = state_counts_by_year(dashboard)

    q_cmap = _US_Q_PALETTE
    q_norm = _US_Q_NORM

    _ax_w = _L_MAP_RECT[2] * FIG_US_W
    _ax_h = _L_MAP_RECT[3] * FIG_US_H
    # x_shift_frac=0.10 shifts view left 10% → map content moves right 10%
    _us_xlim, _us_ylim = _us_view_limits(gdf_proj, _ax_w, _ax_h, x_shift_frac=0.10)

    for year in YEARS:
        raw = counts_raw.get(year, {})
        gdf_year = gdf_proj.copy()
        # Within-year quintile: rank states by articles/million THIS year only
        gdf_year['quintile'] = _year_quintile_col(gdf_year, name_col, raw)

        fig = plt.figure(figsize=(FIG_US_W, FIG_US_H))
        fig.patch.set_facecolor(BG_COLOR)
        ax = fig.add_axes(_L_MAP_RECT)
        ax.set_facecolor(BG_COLOR)

        # Fill: no-data states first, then quintile-coloured states
        no_data = gdf_year[gdf_year['quintile'] == 0]
        no_data.plot(ax=ax, color=NO_DATA, linewidth=0, edgecolor='none')
        has_data = gdf_year[gdf_year['quintile'] > 0]
        if not has_data.empty:
            has_data.plot(ax=ax, column='quintile', cmap=q_cmap, norm=q_norm,
                          linewidth=0, edgecolor='none')
        # Border overlay: always visible regardless of fill shade
        gdf_year.plot(ax=ax, color='none', edgecolor='#999', linewidth=0.4, zorder=5)

        ax.set_axis_off()
        ax.set_xlim(*_us_xlim)
        ax.set_ylim(*_us_ylim)

        _add_overlay_bg(fig, [0.05, _L_YEAR_Y - 0.005, 0.90, 0.065], pad_y=0.005, alpha=0.75)
        fig.text(0.5, _L_TITLE_Y, 'U.S. Coverage in the New York Times',
                 ha='center', va='bottom', fontsize=17, color='#333333', fontfamily='serif',
                 zorder=6)
        fig.text(0.5, _L_YEAR_Y, year,
                 ha='center', va='bottom', fontsize=14, fontweight='bold',
                 color='#1a1a1a', fontfamily='serif', zorder=6)

        _add_overlay_bg(fig, _L_CBAR_RECT, pad_x=0.015, pad_y=0.014, alpha=0.80)
        cbar_ax = fig.add_axes(_L_CBAR_RECT, zorder=5)
        sm = plt.cm.ScalarMappable(cmap=q_cmap, norm=q_norm)
        sm.set_array([])
        cbar = fig.colorbar(sm, cax=cbar_ax, orientation='horizontal')
        cbar.set_ticks([1, 2, 3, 4, 5])
        cbar.set_ticklabels(['Least', '2nd', '3rd', '4th', 'Most'])
        cbar.set_label('Coverage per million residents (within-year quintiles)',
                       fontsize=9, color='#444')
        cbar.ax.tick_params(labelsize=7.5, colors='#444')

        out_path = os.path.join(out_dir, f'us_{year}.png')
        fig.savefig(out_path, dpi=150, facecolor=BG_COLOR, edgecolor='none')
        plt.close(fig)
        print(f'  Saved {out_path}')

# ── Instagram portrait maps ───────────────────────────────────────────────────
def _ig_fig():
    """Return a blank portrait figure with background set."""
    fig = plt.figure(figsize=(FIG_IG_W, FIG_IG_H))
    fig.patch.set_facecolor(BG_COLOR)
    return fig

def _ig_title(fig, year, subtitle):
    # Title (descriptive) on top, larger; year below, smaller
    fig.text(0.5, IG_TITLE_Y,    subtitle, ha='center', va='bottom',
             fontsize=16, color='#333333', fontfamily='serif')
    fig.text(0.5, IG_SUBTITLE_Y, year,     ha='center', va='bottom',
             fontsize=14, fontweight='bold', color='#1a1a1a', fontfamily='serif')

def _ig_cbar(fig, cmap, norm, label):
    cbar_ax = fig.add_axes(IG_CBAR_RECT)
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, cax=cbar_ax, orientation='horizontal')
    cbar.set_label(label, fontsize=8, color='#555')
    cbar.ax.tick_params(labelsize=7, colors='#555')

def make_world_maps_insta(dashboard, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    gdf = get_world_gdf()
    gdf = gdf[gdf['CONTINENT'] != 'Antarctica'].copy()
    gdf = gdf.to_crs('ESRI:54030')

    counts = world_counts_by_year(dashboard)
    norm   = make_log_norm(counts)
    cmap   = plt.get_cmap(CMAP_NAME)

    for year in YEARS:
        year_counts = counts.get(year, {})
        gdf['count'] = gdf['ADMIN'].map(year_counts).fillna(0)

        fig = _ig_fig()
        ax  = fig.add_axes(IG_MAP_RECT)
        ax.set_facecolor(BG_COLOR)

        gdf[gdf['count'] == 0].plot(ax=ax, color=NO_DATA, linewidth=0.3, edgecolor='white')
        has_data = gdf[gdf['count'] > 0].copy()
        if not has_data.empty:
            has_data.plot(ax=ax, column='count', cmap=cmap, norm=norm,
                          linewidth=0.3, edgecolor='white')

        ax.set_axis_off()
        ax.set_xlim(gdf.total_bounds[[0, 2]])
        ax.set_ylim(gdf.total_bounds[[1, 3]])

        _ig_title(fig, year, 'World Coverage in the New York Times')
        _ig_cbar(fig, cmap, norm, 'Articles mentioning country (World section)')

        out_path = os.path.join(out_dir, f'world_{year}.png')
        fig.savefig(out_path, dpi=150, facecolor=BG_COLOR, edgecolor='none')
        plt.close(fig)
        print(f'  Saved {out_path}')

def make_us_maps_insta(dashboard, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    gdf_raw = gpd.read_file(os.path.join(DATA_DIR, 'us_states.geojson'))

    name_col = next((c for c in ['name','NAME','state','STATE','State']
                     if c in gdf_raw.columns), None)
    if name_col is None:
        raise ValueError(f'Cannot find state name column in: {gdf_raw.columns.tolist()}')

    gdf_proj = build_us_gdf(gdf_raw, name_col)
    counts_raw = state_counts_by_year(dashboard)

    q_cmap = _US_Q_PALETTE
    q_norm = _US_Q_NORM

    # Pre-compute view limits: portrait insta rect, 10% right shift
    _ig_ax_w = IG_MAP_RECT[2] * FIG_IG_W
    _ig_ax_h = IG_MAP_RECT[3] * FIG_IG_H
    _ig_xlim, _ig_ylim = _us_view_limits(gdf_proj, _ig_ax_w, _ig_ax_h, x_shift_frac=0.10)

    for year in YEARS:
        raw = counts_raw.get(year, {})
        gdf_year = gdf_proj.copy()
        gdf_year['quintile'] = _year_quintile_col(gdf_year, name_col, raw)

        fig = _ig_fig()
        ax  = fig.add_axes(IG_MAP_RECT)
        ax.set_facecolor(BG_COLOR)

        no_data = gdf_year[gdf_year['quintile'] == 0]
        no_data.plot(ax=ax, color=NO_DATA, linewidth=0, edgecolor='none')
        has_data = gdf_year[gdf_year['quintile'] > 0]
        if not has_data.empty:
            has_data.plot(ax=ax, column='quintile', cmap=q_cmap, norm=q_norm,
                          linewidth=0, edgecolor='none')
        gdf_year.plot(ax=ax, color='none', edgecolor='#999', linewidth=0.4, zorder=5)

        ax.set_axis_off()
        ax.set_xlim(*_ig_xlim)
        ax.set_ylim(*_ig_ylim)

        _ig_title(fig, year, 'U.S. Coverage in the New York Times')
        _ig_cbar(fig, q_cmap, q_norm, 'Coverage per million residents (within-year quintiles)')

        out_path = os.path.join(out_dir, f'us_{year}.png')
        fig.savefig(out_path, dpi=150, facecolor=BG_COLOR, edgecolor='none')
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

    print(f'\nGenerating world Instagram maps ({len(YEARS)} years)...')
    make_world_maps_insta(dashboard, os.path.join(OUTPUT_DIR, 'maps', 'world_insta'))

    print(f'\nGenerating U.S. Instagram maps ({len(YEARS)} years)...')
    make_us_maps_insta(dashboard, os.path.join(OUTPUT_DIR, 'maps', 'us_insta'))

    print('\nDone.')
