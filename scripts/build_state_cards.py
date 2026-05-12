"""Generate per-state Twitter-thread cards: state outline + top-3 recurring
themes with overrepresentation factor and share of state coverage.

Aesthetic: cream background, dark slate ink, serif typography — matches the
'Longest NYT Obituaries' visual family.

Format: 1600x900 (16:9) — Twitter shows this without cropping in feed.

Run from project root:
    python3 scripts/build_state_cards.py [STATE [STATE ...]]
    python3 scripts/build_state_cards.py Wyoming Iowa     # prototype 2 cards
    python3 scripts/build_state_cards.py                  # all 50 + DC

Output: -documents/top-keyword/<state-slug>.png
"""

import os
import sys
import json
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle, FancyBboxPatch
import matplotlib.patches as mpatches
from matplotlib.path import Path
import geopandas as gpd

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_DIR, 'scripts'))
from build_state_keywords import load_articles, analyze

# Display-name overrides for very long subject tags. Keep a short editorial
# rewrite that preserves meaning. The underlying tag in the data is unchanged.
THEME_DISPLAY = {
    'Sixteenth Street Baptist Church (Birmingham, Ala)': '16th St. Baptist Church',
    'Fundamentalist Church of Jesus Christ of Latter-day Saints': 'FLDS Church',
    'Murrah, Alfred P, Federal Building (Oklahoma City)': 'Murrah Federal Building',
    'Mormons (Church of Jesus Christ of Latter-day Saints)': 'Mormons',
    'Mormons (Church of Jesus Christ of Latter-Day Saints)': 'Mormons',
    'Mad Cow Disease (Bovine Spongiform Encephalopathy)': 'Mad Cow Disease',
    'Voter Fraud (Election Fraud)': 'Voter Fraud',
    'Antifa Movement (US)': 'Antifa Movement',
    'Green Cards (US)': 'Green Cards',
    'Recall (Elections)': 'Recall Elections',
    'Nitrogen': 'Execution by Nitrogen Gas',
    'Bourbon (Whiskey)': 'Bourbon',
    "Leprosy (Hansen's Disease)": 'Leprosy',
    'Swatting (Crime)': 'Swatting',
    'Drag (Performance)': 'Drag Performance',
    'Medal of Honor (US)': 'Medal of Honor',
    'Hornets (Insects)': 'Murder Hornets',
    'Japan-International Relations-US': 'US-Japan Relations',
    'Capitol Building (Washington, DC)': 'Capitol Building',
    'US Federal Takeover of Metropolitan Police Dept (DC)': 'Federal Takeover of D.C. Police',
    'Hanford Nuclear Reservation (Wash)': 'Hanford Nuclear Reservation',
    'Logan International Airport (Boston)': 'Logan Airport',
    'Life-Sustaining Support Systems, Withdrawal Of': 'Life-Sustaining Support, Withdrawal',
    'Cuba-International Relations-US': 'US-Cuba Relations',
    'Police Brutality, Misconduct and Shooting': 'Police Misconduct',
    'Recent Commercial Real Estate Transactions': 'Commercial Real Estate Transactions',
    'Voter Registration and Requirements': 'Voter Registration',
    'Greeneville (Submarine)': 'USS Greeneville',
    'Spirit Lake Tribe (Sioux)': 'Spirit Lake Tribe',
    'North American Free Trade Agreement': 'NAFTA',
    'Eighth Amendment (US Constitution)': 'Eighth Amendment',
    'Fourteenth Amendment (US Constitution)': 'Fourteenth Amendment',
    'Tea Party Movement': 'Tea Party Movement',
    'SARS (Severe Acute Respiratory Syndrome)': 'SARS',
    'Indians, American': 'Native Americans',
    'Arizona Immigration Law (SB 1070)': 'Arizona Immigration Law',
}

CREAM = '#f4efe6'
INK = '#2a2a2a'
MUTED = '#7a7368'
ACCENT = '#3a4f5c'   # restrained navy/teal — single accent color
STATE_FILL = '#e8e1d2'
TITLE_BLUE = '#326891'  # the state-name highlight in the title

# State capitals as (city name, longitude, latitude). Used to mark a star
# inside each state outline for geographic anchoring.
STATE_CAPITALS = {
    'Alabama':       ('Montgomery',     -86.2790, 32.3617),
    'Alaska':        ('Juneau',        -134.4197, 58.3019),
    'Arizona':       ('Phoenix',       -112.0740, 33.4484),
    'Arkansas':      ('Little Rock',    -92.2896, 34.7465),
    'California':    ('Sacramento',    -121.4944, 38.5816),
    'Colorado':      ('Denver',        -104.9903, 39.7392),
    'Connecticut':   ('Hartford',       -72.6734, 41.7637),
    'Delaware':      ('Dover',          -75.5277, 39.1582),
    'D.C.':          ('Washington',     -77.0369, 38.9072),
    'Florida':       ('Tallahassee',    -84.2807, 30.4383),
    'Georgia':       ('Atlanta',        -84.3880, 33.7490),
    'Hawaii':        ('Honolulu',      -157.8583, 21.3099),
    'Idaho':         ('Boise',         -116.2023, 43.6150),
    'Illinois':      ('Springfield',    -89.6501, 39.7817),
    'Indiana':       ('Indianapolis',   -86.1581, 39.7684),
    'Iowa':          ('Des Moines',     -93.6250, 41.5868),
    'Kansas':        ('Topeka',         -95.6890, 39.0473),
    'Kentucky':      ('Frankfort',      -84.8733, 38.2009),
    'Louisiana':     ('Baton Rouge',    -91.1871, 30.4515),
    'Maine':         ('Augusta',        -69.7795, 44.3106),
    'Maryland':      ('Annapolis',      -76.4922, 38.9784),
    'Massachusetts': ('Boston',         -71.0589, 42.3601),
    'Michigan':      ('Lansing',        -84.5555, 42.7325),
    'Minnesota':     ('Saint Paul',     -93.0900, 44.9537),
    'Mississippi':   ('Jackson',        -90.1848, 32.2988),
    'Missouri':      ('Jefferson City', -92.1735, 38.5767),
    'Montana':       ('Helena',        -112.0362, 46.5891),
    'Nebraska':      ('Lincoln',        -96.6852, 40.8136),
    'Nevada':        ('Carson City',   -119.7674, 39.1638),
    'New Hampshire': ('Concord',        -71.5376, 43.2081),
    'New Jersey':    ('Trenton',        -74.7563, 40.2206),
    'New Mexico':    ('Santa Fe',      -105.9378, 35.6870),
    'New York':      ('Albany',         -73.7562, 42.6526),
    'North Carolina':('Raleigh',        -78.6382, 35.7796),
    'North Dakota':  ('Bismarck',      -100.7837, 46.8083),
    'Ohio':          ('Columbus',       -82.9988, 39.9612),
    'Oklahoma':      ('Oklahoma City',  -97.5164, 35.4676),
    'Oregon':        ('Salem',         -123.0351, 44.9429),
    'Pennsylvania':  ('Harrisburg',     -76.8839, 40.2732),
    'Rhode Island':  ('Providence',     -71.4128, 41.8240),
    'South Carolina':('Columbia',       -81.0349, 34.0007),
    'South Dakota':  ('Pierre',        -100.3506, 44.3683),
    'Tennessee':     ('Nashville',      -86.7816, 36.1627),
    'Texas':         ('Austin',         -97.7431, 30.2672),
    'Utah':          ('Salt Lake City',-111.8910, 40.7608),
    'Vermont':       ('Montpelier',     -72.5778, 44.2601),
    'Virginia':      ('Richmond',       -77.4360, 37.5407),
    'Washington':    ('Olympia',       -122.9007, 47.0379),
    'West Virginia': ('Charleston',     -81.6326, 38.3498),
    'Wisconsin':     ('Madison',        -89.4012, 43.0731),
    'Wyoming':       ('Cheyenne',      -104.8202, 41.1400),
}


def fmt_pct(p):
    """1 decimal under 2%, 0 decimals at or above. Per Ted's preference."""
    return f"{p:.1f}%" if p < 2 else f"{p:.0f}%"

def display_name(tag):
    if tag in THEME_DISPLAY:
        return THEME_DISPLAY[tag]
    # Generic: drop trailing "(US)" suffix — almost always redundant in
    # the context of a U.S. coverage analysis.
    if tag.endswith(' (US)'):
        return tag[:-len(' (US)')]
    return tag


def slugify(name):
    return name.lower().replace(' ', '-').replace('.', '').replace(',', '')


def make_card(state, recurring, output_path, states_gdf, n_themes=5,
              treatment='hatched'):
    """Render one state card. recurring is list of {tag, score, pct, count}.

    treatment: how to render the state shape — 'plain', 'hatched', 'shadow'
    """
    # Locate the state geometry
    name_field = 'NAME' if 'NAME' in states_gdf.columns else 'name'
    geom_row = states_gdf[states_gdf[name_field] == state]
    if geom_row.empty:
        if state == 'D.C.':
            geom_row = states_gdf[states_gdf[name_field] == 'District of Columbia']
    if geom_row.empty:
        print(f'  Skipping {state} — no geometry')
        return False
    geom = geom_row.iloc[0].geometry

    # Alaska's MultiPolygon includes Aleutian islands that wrap past the
    # dateline (positive longitudes near +180), which makes the auto-bbox
    # span half the globe and renders Alaska as a tiny mass with islands
    # spread across an enormous canvas. Drop any sub-polygon that crosses
    # the dateline (i.e. has positive longitudes) — keeps the mainland +
    # eastern Aleutians + Inside Passage.
    alaska_kept_polys = None
    if state == 'Alaska':
        alaska_kept_polys = [p for p in geom.geoms if p.bounds[2] < 0]

    # Project each state to an Albers Equal-Area CRS for correct proportions.
    # Raw lat/lon stretches northern states wide because longitude lines
    # converge toward the poles — that's why Montana, Nebraska, Nevada,
    # and Alaska looked flattened. Albers preserves area and is the standard
    # projection for U.S. state maps. Each state uses a CRS appropriate
    # for its position so distortion is minimized.
    if state == 'Alaska':
        target_crs = 'EPSG:3338'    # NAD83 / Alaska Albers
    elif state == 'Hawaii':
        target_crs = 'EPSG:6633'    # NAD83 / Hawaii Albers
    elif state == 'D.C.':
        target_crs = 'EPSG:5070'    # NAD83 / Conus Albers
    else:
        target_crs = 'EPSG:5070'

    # 16:9 figure
    fig = plt.figure(figsize=(16, 9), dpi=100, facecolor=CREAM)

    # ──────── Combined title — centered, two-part typography ────────
    # Title prefix is regular weight in INK; the state name is bigger,
    # bold, blue, and underlined — making it the visual anchor of every
    # card. matplotlib doesn't support mixed-style runs in a single
    # text call, so we place two text artists side-by-side and centre
    # the pair as a unit.
    title_prefix = "How The New York Times Covers "
    state_token = state.upper()
    PREFIX_FS = 36
    STATE_FS = 44
    y_title = 0.905

    # Render off-screen first to measure widths in figure coords
    inv = fig.transFigure.inverted()
    p_temp = fig.text(0, 0, title_prefix, fontsize=PREFIX_FS,
                      family='serif', weight='semibold')
    s_temp = fig.text(0, 0, state_token, fontsize=STATE_FS,
                      family='serif', weight='bold')
    fig.canvas.draw()
    pb, sb = p_temp.get_window_extent(), s_temp.get_window_extent()
    pw = inv.transform((pb.x1, 0))[0] - inv.transform((pb.x0, 0))[0]
    sw = inv.transform((sb.x1, 0))[0] - inv.transform((sb.x0, 0))[0]
    p_temp.remove(); s_temp.remove()

    total_w = pw + sw
    start_x = 0.5 - total_w / 2
    fig.text(start_x, y_title, title_prefix, fontsize=PREFIX_FS,
             family='serif', weight='semibold', color=INK,
             ha='left', va='baseline')
    state_artist = fig.text(start_x + pw, y_title, state_token,
                            fontsize=STATE_FS, family='serif',
                            weight='bold', color=TITLE_BLUE,
                            ha='left', va='baseline')

    # Underline the state name — handwritten-style, tucked just below
    # the baseline. All-caps state names have no descenders so the
    # bbox bottom is effectively the baseline.
    fig.canvas.draw()
    sb2 = state_artist.get_window_extent()
    ux0 = inv.transform((sb2.x0, 0))[0]
    ux1 = inv.transform((sb2.x1, 0))[0]
    uy = inv.transform((0, sb2.y0))[1] - 0.006
    underline = plt.Line2D([ux0, ux1], [uy, uy],
                           color=TITLE_BLUE, linewidth=2.5,
                           transform=fig.transFigure,
                           solid_capstyle='round')
    fig.add_artist(underline)

    # ──────── Subhead — slightly more breathing room below title ─────
    fig.text(0.5, 0.838,
             f"Subjects in {state} that The New York Times covers most out "
             f"of proportion to national coverage include:",
             fontsize=18, family='serif', color='#4a4438',
             ha='center')

    # ──────── Left: state outline ────────────────────────────────────
    # Larger canvas — fills the lower-left more dominantly.
    state_ax = fig.add_axes([0.03, 0.10, 0.42, 0.66])
    state_ax.set_facecolor(CREAM)
    state_ax.set_aspect('equal')
    state_ax.axis('off')

    if alaska_kept_polys is not None:
        raw_polys = alaska_kept_polys
    else:
        raw_polys = list(geom.geoms) if geom.geom_type == 'MultiPolygon' else [geom]

    # Project the polygons to the chosen Albers CRS
    if states_gdf.crs is not None and target_crs:
        proj_series = gpd.GeoSeries(raw_polys, crs=states_gdf.crs).to_crs(target_crs)
        polys = list(proj_series)
    else:
        polys = raw_polys

    # Drop shadow — paper-on-paper depth, used by every treatment.
    for poly in polys:
        xs, ys = poly.exterior.xy
        rng_x = max(xs) - min(xs)
        rng_y = max(ys) - min(ys)
        ox = rng_x * 0.010
        oy = rng_y * 0.010
        state_ax.fill([x + ox for x in xs], [y - oy for y in ys],
                      facecolor='#bdb39d', edgecolor='none',
                      alpha=0.55, zorder=1)

    # Main fill
    for poly in polys:
        xs, ys = poly.exterior.xy
        state_ax.fill(xs, ys, facecolor=STATE_FILL, edgecolor='none', zorder=1.5)

    if treatment == 'hatched':
        # Engraving-style diagonal hatching at low contrast over the fill.
        # matplotlib renders hatching only when both facecolor and edgecolor
        # are set, but the edge artifact at the polygon boundary needs to be
        # masked by the crisp outline drawn last.
        import matplotlib.patches as mpatches
        from matplotlib.collections import PatchCollection
        for poly in polys:
            xs, ys = poly.exterior.xy
            # PathPatch lets us control hatching color via edgecolor
            from matplotlib.path import Path as MplPath
            from matplotlib.patches import PathPatch
            verts = list(zip(xs, ys))
            codes = [MplPath.MOVETO] + [MplPath.LINETO] * (len(verts) - 2) + [MplPath.CLOSEPOLY]
            patch = PathPatch(MplPath(verts, codes),
                              facecolor='none',
                              edgecolor='#a89d83',
                              hatch='////',
                              linewidth=0,
                              zorder=2)
            state_ax.add_patch(patch)

    # Crisp outline on top of all treatments
    for poly in polys:
        xs, ys = poly.exterior.xy
        state_ax.plot(xs, ys, color=INK, linewidth=2.5, solid_joinstyle='round',
                      zorder=3)

    # Capital city — unlabeled star, just a small bit of color/anchor.
    # Project the capital point through the same CRS as the state polygon.
    cap = STATE_CAPITALS.get(state)
    if cap is not None:
        from shapely.geometry import Point
        _, lon, lat = cap
        cap_proj = (gpd.GeoSeries([Point(lon, lat)], crs=states_gdf.crs)
                    .to_crs(target_crs).iloc[0])
        state_ax.plot([cap_proj.x], [cap_proj.y], marker='*', color='#a83a2c',
                      markersize=14, markeredgecolor='#5a1a0e',
                      markeredgewidth=0.8, zorder=4)

    # Auto-scale to state bounds (computed from kept polys, not from
    # geom.bounds — important for Alaska where geom still includes the
    # dateline-crossing Aleutians).
    all_x = [c for p in polys for c in p.exterior.xy[0]]
    all_y = [c for p in polys for c in p.exterior.xy[1]]
    xmin, xmax = min(all_x), max(all_x)
    ymin, ymax = min(all_y), max(all_y)
    pad_x = (xmax - xmin) * 0.06
    pad_y = (ymax - ymin) * 0.06
    state_ax.set_xlim(xmin - pad_x, xmax + pad_x)
    state_ax.set_ylim(ymin - pad_y, ymax + pad_y)

    # ──────── Right: theme list ──────────────────────────────────────
    items = recurring[:n_themes]
    n = len(items)
    if n == 0:
        fig.text(0.48, 0.40, '(no qualifying themes)',
                 fontsize=18, family='serif', color=MUTED, style='italic')
    else:
        # Distribute themes evenly across a tighter vertical band — pulled
        # in from both top and bottom to give breathing room above the
        # methods footer.
        y_top = 0.70
        y_bot = 0.20
        if n == 1:
            ys = [(y_top + y_bot) / 2]
        else:
            ys = [y_top - i * (y_top - y_bot) / (n - 1) for i in range(n)]

        # No numbering — score and share are both shown, so readers can
        # re-rank in their head. Skipping the numerals removes ink.
        for t, y in zip(items, ys):
            disp = display_name(t['tag'])
            fig.text(0.48, y, disp,
                     fontsize=24, weight='bold', family='serif', color=INK)
            stats = (f"{int(round(t['score']))}× as common as in U.S. coverage  ·  "
                     f"{fmt_pct(t['pct'])} of state articles")
            fig.text(0.48, y - 0.035, stats,
                     fontsize=12, family='serif', color=MUTED)

    # ──────── Methods footer — single centered line ──────────────────
    # Sized so the line spans nearly the full bottom width with the same
    # side margin as the title.
    fig.text(0.5, 0.055,
             "Methods: Analysis of The New York Times-assigned keywords in "
             "U.S. coverage between 2000–2026"
             "  ·  Data from NYT Archive API"
             "  ·  tedalcorn.github.io/nyt",
             fontsize=14, ha='center', family='serif', color='#7a7368')

    # No bbox_inches='tight' — the tight crop used the state-outline's
    # leftmost coordinate as the left edge, leaving the title (centered at
    # figure-x=0.5) visually offset toward the right. Saving at the exact
    # 16x9 inches × 100dpi keeps the cream background as natural margin and
    # the title actually centered.
    plt.savefig(output_path, dpi=100, facecolor=CREAM)
    plt.close()
    return True


def main():
    # Resolve states to render
    targets = sys.argv[1:] if len(sys.argv) > 1 else None

    print('Loading state geometries…')
    geo_path = os.path.join(PROJECT_DIR, 'data', 'us_states.geojson')
    states_gdf = gpd.read_file(geo_path)
    print(f'  {len(states_gdf)} state polygons')

    print('Running theme analysis…')
    arts = load_articles()
    res = analyze(arts)
    print(f'  {len(res)} states scored')

    out_dir = os.path.join(PROJECT_DIR, '-documents', 'top-keyword')
    os.makedirs(out_dir, exist_ok=True)

    if targets is None:
        targets = sorted(res.keys())

    for state in targets:
        if state not in res:
            print(f'  Skipping {state} — no analysis result')
            continue
        recurring = res[state]['recurring']  # full list; make_card slices to n_themes
        out_path = os.path.join(out_dir, f'{slugify(state)}.png')
        ok = make_card(state, recurring, out_path, states_gdf)
        if ok:
            print(f'  ✓ {out_path}')


if __name__ == '__main__':
    main()
