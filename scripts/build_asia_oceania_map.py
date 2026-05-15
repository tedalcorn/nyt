"""Asia + Oceania regional map: each country with sufficient NYT
coverage gets its #1 recurring theme labeled inside its polygon.

Same aesthetic and label-fit algorithm as the other regional maps.
Afghanistan and Pakistan have the 9/11 carve-out applied (Pentagon,
WTC, Hijacking, Airlines stripped from their scoring); the
methodology notes that briefly.

Run:
    python3 scripts/build_asia_oceania_map.py

Output: outputs/2026-05-top-keyword/2026-05-13-world-country-tweets/
        Asia-and-Oceania/asia-and-oceania-map.{png,pdf}
"""
import os
import sys
import math
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import Point, Polygon as ShpPolygon

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_DIR, 'scripts'))
from build_country_keywords import load_world_articles, analyze
from build_country_cards import (display_name, COUNTRY_TO_GEOJSON,
                                 _condense_olympics)

# ── Aesthetic ─────────────────────────────────────────────────────────
CREAM = '#f4efe6'
INK = '#2a2a2a'
MUTED = '#7a7368'
COUNTRY_FILL = '#e8e1d2'
NO_DATA_FILL = '#ded5c4'
BORDER = '#5a5447'
LEADER = '#857c6e'
TITLE_BLUE = '#326891'

# ── Countries on this map (geojson NAME values) ───────────────────────
ASIA_COUNTRIES = {
    # West / Caucasus (Turkey lives on the Africa+ME map)
    'Armenia','Azerbaijan','Georgia',
    # Central Asia
    'Kazakhstan','Kyrgyzstan','Tajikistan','Turkmenistan','Uzbekistan',
    # South Asia
    'Afghanistan','Bangladesh','Bhutan','India','Nepal','Pakistan','Sri Lanka',
    # East Asia
    'China','Japan','Mongolia','North Korea','South Korea','Taiwan',
    # Southeast Asia
    'Brunei','Cambodia','Indonesia','Laos','Malaysia','Myanmar','Philippines',
    'Thailand','Timor-Leste','Vietnam',
    # North
    'Russia',
    # Oceania
    'Australia','Fiji','New Caledonia','New Zealand','Papua New Guinea',
    'Solomon Is.','Vanuatu',
}

# geojson NAME → analysis country-name (only when they differ)
GEOJSON_TO_ANALYSIS = {
    'Timor-Leste': 'East Timor',
    'Solomon Is.': 'Solomon Islands',
    'Myanmar': 'Myanmar',  # alias of Burma in our data, but analysis uses Myanmar
}

# Short display names for the country caption
COUNTRY_DISPLAY_NAME = {
    'Solomon Is.': 'Solomon Is.',
    'Papua New Guinea': 'P.N.G.',
    'Timor-Leste': 'Timor-Leste',
    'North Korea': 'N. Korea',
    'South Korea': 'S. Korea',
}

# Per-country fit overrides. forced_text trims long parenthetical
# tag names and ensures small countries get readable labels.
ASIA_OVERRIDES = {
    # East Asia
    'Russia':       {'forced_text': 'Cold War Era', 'fs_max': 56,
                     'rotations': [0]},
    # Han Chinese (majority ethnic group) excluded in
    # COUNTRY_TAG_EXCLUSIONS — China's #1 is now Uighurs.
    # anchor_y_frac=0.45 nudges it a bit down so it doesn't ride into Mongolia.
    'China':        {'forced_text': 'Uighurs', 'fs_max': 48,
                     'rotations': [-15, 0],
                     'anchor_y_frac': 0.42},
    'Japan':        {'forced_text': 'Typhoons', 'fs_max': 18,
                     'rotations': [-60, 0]},
    'Mongolia':     {'fs_max': 22, 'rotations': [0]},  # no data
    'Taiwan':       {'forced_text': 'Computer\nChips', 'fs_max': 8,
                     'rotations': [0]},  # also a callout fallback
    # South Asia
    'India':        {'forced_text': 'Hinduism', 'fs_max': 44,
                     'rotations': [-10, 0]},
    'Pakistan':     {'forced_text': 'Pashtun', 'fs_max': 22,
                     'rotations': [10]},
    'Afghanistan':  {'forced_text': 'Opium', 'fs_max': 20,
                     'rotations': [10]},
    'Bangladesh':   {'forced_text': 'Rakhine\nEthnic Group', 'fs_max': 8},
    'Sri Lanka':    {'forced_text': 'Sinhalese\nEthnic Group', 'fs_max': 8},
    # Indonesia handled as a callout with synthetic anchor — see CALLOUT_OFFSETS.
    'Thailand':     {'forced_text': 'Angkor Wat', 'fs_max': 12,
                     'rotations': [-60, 0]},
    'Myanmar':      {'forced_text': 'Rakhine\nEthnic Group', 'fs_max': 12,
                     'rotations': [70, 0]},
    'Cambodia':     {'forced_text': 'Angkor Wat', 'fs_max': 9},
    'Philippines':  {'forced_text': 'Typhoons', 'fs_max': 12,
                     'rotations': [80, 0]},
    'Malaysia':     {'fs_max': 8, 'rotations': [0]},
    'Vietnam':      {'forced_text': 'Vietnamese-\nAmericans', 'fs_max': 9,
                     'rotations': [-65, 0]},
    # Central Asia
    'Kazakhstan':   {'forced_text': 'Rocket Science\nand Propulsion',
                     'fs_max': 18, 'rotations': [-10, 0]},
    # Uzbeks excluded (majority) — Uzbekistan's #1 after 9/11 carve-out
    # is now "Torture" (the Karimov regime).
    'Uzbekistan':   {'forced_text': 'Torture', 'fs_max': 14,
                     'rotations': [-30, 0]},
    # Tajiks excluded — Tajikistan's #1 after 9/11 carve-out is now
    # "Drug Trafficking" (heroin route from Afghanistan).
    'Tajikistan':   {'forced_text': 'Drug\nTrafficking', 'fs_max': 7},
    # Kyrgyzstan handled as callout — see CALLOUT_OFFSETS.
    # South Korea inline if it fits; otherwise callout
    'South Korea':  {'forced_text': 'K-Pop', 'fs_max': 9,
                     'rotations': [-45, 0]},
    # Oceania
    'Australia':       {'forced_text': 'Coral', 'fs_max': 56,
                        'rotations': [0]},
    'New Zealand':     {'forced_text': 'Maoris', 'fs_max': 11,
                        'rotations': [-65, 0]},
    'Papua New Guinea': {'forced_text': 'Immigration\nDetention',
                         'fs_max': 11, 'rotations': [0]},
    # Nepal handled via callout
}

# Small countries → callouts
# (dx, dy[, rotation[, forced_text[, ha_override]]])
CALLOUT_OFFSETS = {
    # Schema: (dx, dy, rotation, text, ha, term_fs, name_fs[, show_leader])
    # Japan: rotate 30° CCW. dx pushed further right (now positive) so
    # the label sits in the Pacific east-southeast of Honshu.
    'Japan':        (0.010, -0.035, 30, 'Typhoons', 'center', 20, 13, False),
    # Taiwan: rotated 85° CCW, wrapped, pushed further east.
    'Taiwan':       (0.030, 0.004, 85, 'Computer\nChips', 'center', 17, 10, False),
    # South Korea callout into Sea of Japan.
    'South Korea':  (0.030, 0.035, 0, 'K-Pop', 'left'),
    # Philippines: rotated -70° to match archipelago axis.
    'Philippines':  (0.048, -0.040, -70, 'Typhoons', 'center', 15, 10, False),
    # SE Asia mainland callouts
    # Thailand: 2px more left + 3px up.
    'Thailand':     (-0.050, -0.033, 0, 'Angkor Wat', 'center'),
    'Cambodia':     (0.034, -0.038, 10, 'Angkor Wat', 'center', 12, 9),
    # Vietnam: 2px more left to be closer to the coast.
    'Vietnam':      (0.020, -0.005, 5, 'Vietnamese-\nAmericans', 'left'),
    # Bangladesh: nudged 3px further right to clear the India border.
    'Bangladesh':   (-0.022, -0.052, 0, 'Rakhine\nEthnic Group', 'center'),
    # Malaysia — pushed 2px more up so the label clears the landmass.
    'Malaysia':     (0.046, -0.004, 0, 'Rohingya\nEthnic Group', 'left'),
    # Indonesia: rotated 20° CW (flatter), close under Java.
    'Indonesia':    (0.0, 0.0, -20, 'Black Boxes', 'center', 24, 14, False),
    # NZ: nudged further up + right, Maoris bumped to fs=22.
    'New Zealand':  (-0.006, 0.018, 15, 'Maoris', 'center', 22, 12, False),
    # Sri Lanka: callout now to the EAST of the island (in the Bay of
    # Bengal), not west. New #1 is Tamils (Sinhalese majority excluded).
    'Sri Lanka':    (0.020, -0.024, 0, 'Tamils', 'left'),
    # Nepal: 1px up to clear the China-Nepal border.
    'Nepal':        (0.005, 0.022, 0, 'Sherpas', 'center'),
    # Kyrgyzstan: callout pushed down + right into China/Xinjiang
    # territory where there's open space to read.
    'Kyrgyzstan':   (0.012, -0.025, 0, 'Uzbeks', 'left'),
    # Pacific micro-states
    'Fiji':         (0.020, 0.005),
    'Vanuatu':      (-0.025, 0.000),
    'New Caledonia': (0.025, -0.010),
    'Solomon Is.':  (-0.030, 0.020),
    'Brunei':       (0.020, 0.010),
    'Timor-Leste':  (0.025, -0.015),
    'Bhutan':       (-0.020, 0.025, 0, 'Bhutan', 'right'),
    # Caucasus tight cluster
    # Armenia: shifted right + down so caption fits on-canvas. After
    # excluding "Armenians" (majority), Armenia's #1 is "Ottoman Empire".
    'Armenia':      (-0.027, -0.031, 0, 'Ottoman\nEmpire', 'right'),
    # Azerbaijan: 2px further left, leader hidden (was crossing the text).
    'Azerbaijan':   (-0.014, -0.045, 0, None, 'left', None, None, False),
    # Georgia: wrap "Gas (Fuel)" onto 2 lines.
    'Georgia':      (-0.025, 0.030, 0, 'Gas\n(Fuel)', 'right'),
}

# Skip-countries set — special exclusions
SKIP_COUNTRIES = set()

# Bbox in lat/lon — covers Asia (lon 30 W of Caucasus to Pacific ~190°E
# so the LAEA projection of NZ doesn't get clipped) and Oceania
# (Australia/NZ down to lat -50). Russia north up to ~72.
# Western edge moved to 35 since Turkey dropped from this map.
ASIA_BBOX_LATLON = (35, -50, 190, 72)

MIN_SCORE_TO_LABEL = 6.0
MIN_TAG_YEAR_SPAN = 2


def pick_asia_projection():
    """Lambert Azimuthal Equal-Area centered on the bbox centroid
    (lat≈10, lon≈100) so Russia at the top and Australia/NZ at the
    bottom are roughly equidistant from the projection center."""
    return '+proj=laea +lat_0=10 +lon_0=100 +datum=WGS84 +units=m +no_defs'


def get_country_polys(world_gdf, target_crs):
    name_field = 'NAME' if 'NAME' in world_gdf.columns else 'name'
    out = {}
    for _, row in world_gdf.iterrows():
        name = row[name_field]
        if name not in ASIA_COUNTRIES:
            continue
        geom = row.geometry
        raw = list(geom.geoms) if geom.geom_type == 'MultiPolygon' else [geom]
        # Drop far-flung overseas / outlying islands
        if len(raw) > 1:
            big = max(raw, key=lambda p: p.area)
            bx0, by0, bx1, by1 = big.bounds
            bcx, bcy = (bx0+bx1)/2, (by0+by1)/2
            raw = [p for p in raw
                   if abs((p.bounds[0]+p.bounds[2])/2 - bcx) <= 30
                   and abs((p.bounds[1]+p.bounds[3])/2 - bcy) <= 30]
        if not raw:
            continue
        proj = gpd.GeoSeries(raw, crs=world_gdf.crs).to_crs(target_crs)
        out[name] = list(proj)
    return out


def wrap_options(label, max_lines=4):
    words = label.split()
    yield 1, label
    if len(words) <= 1:
        return
    for n in range(2, min(max_lines, len(words)) + 1):
        per = len(words) // n
        extra = len(words) % n
        out, i = [], 0
        for k in range(n):
            count = per + (1 if k < extra else 0)
            out.append(' '.join(words[i:i + count]))
            i += count
        yield n, '\n'.join(out)


def measure_text_size(ax, fig, text, fs):
    lines = text.split('\n')
    n_lines = len(lines)
    max_chars = max(len(l) for l in lines)
    width_pt = max_chars * fs * 0.62
    height_pt = n_lines * fs * 1.18
    dpi = fig.dpi
    width_px = width_pt * dpi / 72.0
    height_px = height_pt * dpi / 72.0
    inv = ax.transData.inverted()
    (x0, y0) = inv.transform((0, 0))
    (x1, y1) = inv.transform((width_px, height_px))
    return abs(x1 - x0), abs(y1 - y0)


def rotated_text_box(cx, cy, w, h, angle_deg):
    a = math.radians(angle_deg)
    cos_a, sin_a = math.cos(a), math.sin(a)
    hw, hh = w / 2, h / 2
    corners_local = [(-hw, -hh), (hw, -hh), (hw, hh), (-hw, hh)]
    corners = [(cx + x * cos_a - y * sin_a,
                cy + x * sin_a + y * cos_a) for x, y in corners_local]
    return ShpPolygon(corners)


def candidate_anchors(poly, anchor_y_frac=None, anchor_x_frac=None):
    pts = [poly.representative_point(), poly.centroid]
    minx, miny, maxx, maxy = poly.bounds
    if anchor_y_frac is not None or anchor_x_frac is not None:
        cy = miny + (maxy - miny) * (anchor_y_frac if anchor_y_frac is not None else 0.5)
        cx = minx + (maxx - minx) * (anchor_x_frac if anchor_x_frac is not None else 0.5)
        candidate = Point(cx, cy)
        if poly.contains(candidate):
            pts.insert(0, candidate)
    try:
        mrr = poly.convex_hull.minimum_rotated_rectangle
        coords = list(mrr.exterior.coords)
        edges = [(coords[i], coords[i+1]) for i in range(4)]
        longest = max(edges, key=lambda e: math.hypot(e[1][0]-e[0][0], e[1][1]-e[0][1]))
        angle = math.atan2(longest[1][1]-longest[0][1], longest[1][0]-longest[0][0])
    except Exception:
        angle = 0
    cx, cy = poly.centroid.x, poly.centroid.y
    diag = math.hypot(maxx-minx, maxy-miny)
    for t in (-0.30, -0.15, 0.15, 0.30):
        px = cx + t * diag * math.cos(angle)
        py = cy + t * diag * math.sin(angle)
        p = Point(px, py)
        if poly.contains(p):
            pts.append(p)
    seen = set()
    for p in pts:
        key = (round(p.x, 2), round(p.y, 2))
        if key in seen:
            continue
        seen.add(key)
        if poly.contains(p):
            yield p


def text_fit_score(rect, poly):
    if poly.contains(rect):
        return 1.0
    inter = poly.intersection(rect)
    if inter.is_empty:
        return 0.0
    return inter.area / rect.area


def fit_label(ax, fig, poly, label, override):
    fs_max = override.get('fs_max', 22)
    fs_min = override.get('fs_min', 6)
    rotations = override.get('rotations', [0])
    anchor_y_frac = override.get('anchor_y_frac')
    anchor_x_frac = override.get('anchor_x_frac')
    fit_threshold = override.get('fit_threshold', 0.97)
    forced_text = override.get('forced_text')

    anchors = list(candidate_anchors(poly, anchor_y_frac=anchor_y_frac,
                                     anchor_x_frac=anchor_x_frac))
    if not anchors:
        return None
    if forced_text:
        options = [(forced_text.count('\n') + 1, forced_text)]
    else:
        options = list(wrap_options(label, max_lines=4))

    best = None
    for fs in range(fs_max, fs_min - 1, -1):
        for n_lines, text in options:
            w, h = measure_text_size(ax, fig, text, fs)
            for rot_idx, rotation in enumerate(rotations):
                for anchor in anchors:
                    rect = rotated_text_box(anchor.x, anchor.y, w, h, rotation)
                    score = text_fit_score(rect, poly)
                    if score >= fit_threshold:
                        return (anchor.x, anchor.y, fs, rotation, text)
                    if best is None or score > best[0]:
                        best = (score, anchor.x, anchor.y, fs, rotation, text)
    if best and best[0] >= 0.85 and best[3] >= fs_min:
        return (best[1], best[2], best[3], best[4], best[5])
    return None


def main():
    print('Loading country geometries…')
    geo_path = os.path.join(PROJECT_DIR, 'data', 'world_countries.geojson')
    world_gdf = gpd.read_file(geo_path)
    target_crs = pick_asia_projection()

    print('Running theme analysis…')
    arts = load_world_articles()
    res = analyze(arts)
    print(f'  {len(res)} countries scored')

    polys_by_country = get_country_polys(world_gdf, target_crs)
    print(f'  {len(polys_by_country)} country geometries')

    # Synthetic callout anchors — override biggest.representative_point()
    # for countries where it falls in an awkward place (Indonesia's
    # biggest poly is Borneo, not south Java where we want the label).
    synthetic_anchors = {
        'Indonesia': gpd.GeoSeries(
            [Point(108, -12)], crs=world_gdf.crs).to_crs(target_crs)[0],
    }

    # Compute bbox from the actual projected country geometries (LAEA
    # distortion at far corners means the lat/lon rectangle's projection
    # doesn't enclose all polygons — NZ in particular projects past it).
    all_xs, all_ys = [], []
    for polys in polys_by_country.values():
        for p in polys:
            if not p.exterior:
                continue
            xs, ys = p.exterior.xy
            all_xs.extend(xs)
            all_ys.extend(ys)
    PAD = 200_000  # ~200 km on each side
    # Extra padding on the left so the Caucasus callouts (which extend
    # west of their countries' polygons) don't clip. Shifts everything
    # in the map ~7 preview-px to the right, freeing room for "Armenia:"
    # and "Georgia: Gas (Fuel)" captions on-canvas.
    LEFT_EXTRA = 1_500_000
    bbox_minx = min(all_xs) - PAD - LEFT_EXTRA
    bbox_maxx = max(all_xs) + PAD
    bbox_miny = min(all_ys) - PAD
    bbox_maxy = max(all_ys) + PAD
    eur_w = bbox_maxx - bbox_minx
    eur_h = bbox_maxy - bbox_miny
    bbox_aspect = eur_w / eur_h

    # Asia+Oceania is wider than tall in LAEA — let aspect drive
    # map_w_inches from map_h_inches.
    map_h_inches = 16
    map_w_inches = map_h_inches * bbox_aspect
    TOP_MARGIN_INCHES = 1.7   # title 1 line (~0.65") + 2-line subtitle (~0.70") + buffer
    BOTTOM_MARGIN_INCHES = 0.5   # methodology now lives WITHIN the map's
                                  # lower-left quadrant (over open Indian
                                  # Ocean), so the bottom band can be small.
    fig_w = map_w_inches
    fig_h = map_h_inches + TOP_MARGIN_INCHES + BOTTOM_MARGIN_INCHES
    fig = plt.figure(figsize=(fig_w, fig_h), dpi=120, facecolor=CREAM)
    map_ax_bottom = BOTTOM_MARGIN_INCHES / fig_h
    map_ax_height = map_h_inches / fig_h
    map_ax_top = map_ax_bottom + map_ax_height
    map_ax = fig.add_axes([0.0, map_ax_bottom, 1.0, map_ax_height])
    map_ax.set_facecolor(CREAM)
    map_ax.set_aspect('equal')
    map_ax.axis('off')
    map_ax.set_xlim(bbox_minx, bbox_maxx)
    map_ax.set_ylim(bbox_miny, bbox_maxy)

    # Title on ONE line spanning the figure width; subtitle on 2 lines,
    # allowed to run as wide as the title before wrapping.
    title_y = 1.0 - 0.10 / fig_h
    SUB_LINE_INCH = 0.35
    sub_y1 = title_y - 0.65 / fig_h
    sub_y2 = sub_y1 - SUB_LINE_INCH / fig_h
    title_block_bottom_y = sub_y2 - 0.15 / fig_h

    fig.text(0.02, title_y, "How The New York Times Looks At Asia & Oceania",
             fontsize=28, family='serif', weight='semibold',
             color=INK, ha='left', va='top')
    fig.text(0.02, sub_y1, "Keywords that The New York Times assigns to its articles show which recurring",
             fontsize=19, family='serif', color='#4a4438', ha='left', va='top')
    fig.text(0.02, sub_y2, "subjects are covered in each country out of proportion to international coverage as a whole.",
             fontsize=19, family='serif', color='#4a4438', ha='left', va='top')

    assert title_block_bottom_y > map_ax_top, (
        f"Title block ends at y={title_block_bottom_y:.3f}, "
        f"map top at y={map_ax_top:.3f}; they overlap! "
        f"Increase TOP_MARGIN_INCHES."
    )

    # Draw countries
    for gname, polys in polys_by_country.items():
        analysis_name = GEOJSON_TO_ANALYSIS.get(gname, gname)
        has_data = analysis_name in res
        fill = COUNTRY_FILL if has_data else NO_DATA_FILL
        for poly in polys:
            if not poly.exterior: continue
            xs, ys = poly.exterior.xy
            rng_x = max(xs) - min(xs)
            rng_y = max(ys) - min(ys)
            ox = rng_x * 0.004
            oy = rng_y * 0.004
            map_ax.fill([x + ox for x in xs], [y - oy for y in ys],
                        facecolor='#c2b9a3', edgecolor='none',
                        alpha=0.30, zorder=1)
        for poly in polys:
            if not poly.exterior: continue
            xs, ys = poly.exterior.xy
            if has_data:
                map_ax.fill(xs, ys, facecolor=fill, edgecolor='none', zorder=1.5)
            else:
                # No-data countries get a hatched fill so they read as
                # distinct from labeled-but-not-shown countries.
                map_ax.fill(xs, ys, facecolor=NO_DATA_FILL,
                            edgecolor='#b8ad95', linewidth=0.0,
                            hatch='////', zorder=1.5)
        for poly in polys:
            if not poly.exterior: continue
            xs, ys = poly.exterior.xy
            map_ax.plot(xs, ys, color=BORDER, linewidth=0.7,
                        solid_joinstyle='round', zorder=3)

    # Label countries
    callouts = []
    for gname, polys in polys_by_country.items():
        analysis_name = GEOJSON_TO_ANALYSIS.get(gname, gname)
        if analysis_name in SKIP_COUNTRIES:
            continue
        country_res = res.get(analysis_name, {})
        recurring = country_res.get('recurring', [])
        if not recurring:
            continue
        recurring = _condense_olympics(recurring, country_res.get('tag_years', {}))
        tag_years_map = country_res.get('tag_years', {})
        top = None
        for cand in recurring:
            if cand['score'] < MIN_SCORE_TO_LABEL:
                break
            if len(tag_years_map.get(cand['tag'], {})) >= MIN_TAG_YEAR_SPAN:
                top = cand
                break
        if top is None:
            continue
        label = display_name(top['tag'], analysis_name)

        bbox_rect = ShpPolygon([
            (bbox_minx, bbox_miny), (bbox_maxx, bbox_miny),
            (bbox_maxx, bbox_maxy), (bbox_minx, bbox_maxy),
        ])
        clipped_pieces = []
        for p in polys:
            try:
                c = p.intersection(bbox_rect)
            except Exception:
                # Invalid geometry — repair with buffer(0) and retry.
                try:
                    c = p.buffer(0).intersection(bbox_rect)
                except Exception:
                    continue
            if c.is_empty: continue
            if c.geom_type == 'Polygon':
                clipped_pieces.append(c)
            elif c.geom_type == 'MultiPolygon':
                clipped_pieces.extend(c.geoms)
        if not clipped_pieces:
            continue
        biggest = max(clipped_pieces, key=lambda p: p.area)

        if gname in CALLOUT_OFFSETS:
            anchor = synthetic_anchors.get(gname, biggest.representative_point())
            callouts.append((gname, label, anchor))
            continue

        override = ASIA_OVERRIDES.get(gname, {})
        fit = fit_label(map_ax, fig, biggest, label, override)
        if fit is None:
            callouts.append((gname, label, biggest.representative_point()))
            continue
        cx, cy, fs, rotation, text = fit

        country_caption = COUNTRY_DISPLAY_NAME.get(gname, gname) + ':'
        _, h = measure_text_size(map_ax, fig, text, fs)
        cap_fs = max(8, int(fs * 0.62))
        tight = override.get('tight_caption', False)
        gap_factor = 0.50 if tight else 0.65
        rot_rad = math.radians(rotation)
        offset = h * gap_factor
        offset_x = -math.sin(rot_rad) * offset
        offset_y = math.cos(rot_rad) * offset
        map_ax.text(cx + offset_x, cy + offset_y, country_caption,
                    ha='center', va='center',
                    fontsize=cap_fs, family='serif', weight='normal',
                    color=MUTED, rotation=rotation, zorder=4)
        map_ax.text(cx, cy, text,
                    ha='center', va='center',
                    fontsize=fs, family='serif', weight='semibold',
                    color=INK, rotation=rotation, zorder=4)

    # Callouts with tight name/term stacking + shortened leaders
    def _text_height_data(fs, n_lines=1):
        inv = map_ax.transData.inverted()
        h_pt = fs * 1.18 * n_lines
        h_px = h_pt * fig.dpi / 72
        (_, y0) = inv.transform((0, 0))
        (_, y1) = inv.transform((0, h_px))
        return abs(y1 - y0)

    DEFAULT_NAME_FS = 8
    DEFAULT_TERM_FS = 10
    # Schema extended to: (dx, dy, rotation, text, ha, term_fs, name_fs,
    #                      show_leader)
    for gname, text, anchor in callouts:
        cfg = CALLOUT_OFFSETS.get(gname, (0.04, 0.02))
        dx, dy = cfg[0], cfg[1]
        rotation = cfg[2] if len(cfg) > 2 else 0
        if len(cfg) > 3 and cfg[3]:
            text = cfg[3]
        ha_override = cfg[4] if len(cfg) > 4 else None
        term_fs = cfg[5] if len(cfg) > 5 and cfg[5] else DEFAULT_TERM_FS
        name_fs = cfg[6] if len(cfg) > 6 and cfg[6] else DEFAULT_NAME_FS
        show_leader = cfg[7] if len(cfg) > 7 else True
        lx = anchor.x + dx * eur_w
        ly = anchor.y + dy * eur_h

        if show_leader:
            leader_end_x = anchor.x + 0.85 * (lx - anchor.x)
            leader_end_y = anchor.y + 0.85 * (ly - anchor.y)
            map_ax.plot([anchor.x, leader_end_x], [anchor.y, leader_end_y],
                        color=LEADER, linewidth=0.7, alpha=0.85, zorder=2.5)

        analysis_name = GEOJSON_TO_ANALYSIS.get(gname, gname)
        ha = ha_override if ha_override else ('right' if dx < 0 else 'left')

        n_term_lines = text.count('\n') + 1
        name_h = _text_height_data(name_fs, 1)
        term_h = _text_height_data(term_fs, n_term_lines)
        between = (name_h + term_h) / 2
        rot_rad = math.radians(rotation)
        name_x = lx + (-math.sin(rot_rad)) * between
        name_y = ly + math.cos(rot_rad) * between

        map_ax.text(lx, ly, text,
                    fontsize=term_fs, ha=ha, va='center',
                    family='serif', weight='semibold', color=INK,
                    rotation=rotation, zorder=4)
        map_ax.text(name_x, name_y, analysis_name + ':',
                    fontsize=name_fs, ha=ha, va='center',
                    family='serif', color=MUTED, rotation=rotation, zorder=4)

    # Methodology — sits in a wide band BELOW the map (no convenient
    # open-water strip in this bbox to embed it in). Wider/fewer lines
    # than the AfME version so the block fits in ~2.5" of vertical space.
    n_world_articles = sum(1 for a in arts if (a.get('s') or '') == 'World')
    rounded_articles = f"{round(n_world_articles, -3):,.0f}"
    from matplotlib.offsetbox import HPacker, TextArea, AnnotationBbox
    METH_COLOR = '#4a4438'
    methodology_lines = [
        f'This map draws on {rounded_articles} articles in',
        'the World section from 2000 to 2026. The',
        'New York Times assigns each article subject',
        'keywords (separate from tags for individual',
        'people and organizations, which are not',
        'included here). For each country with sufficient',
        'coverage to identify recurring patterns, the',
        'map shows the keyword that (a) appeared on at',
        'least 1% of the country’s coverage and (b) was',
        '**most** out of proportion with that keyword’s',
        'frequency in World coverage overall. The',
        'analysis excludes each country’s own currency',
        'and majority ethnic group, broad topics applied',
        'to most countries such as “international',
        'relations,” and one-time events such as named',
        'storms, major accidents, and specific Olympic',
        'Games. In Afghanistan and Pakistan, tags related',
        'to 9/11 that dominated coverage in the early',
        '2000s are also excluded so more typical recurring',
        'themes can surface.',
    ]
    METH_X = 0.025
    METH_FS = 15
    # 15pt × 1.25 / 72 ≈ 0.26" per line + buffer:
    LINE_SPACING = 0.30 / fig_h
    # Position: top of methodology at roughly 1/3 from bottom of image,
    # in the lower-LEFT quadrant where the open Indian Ocean sits south
    # of India and west of Indonesia.
    y = 0.38
    for line in methodology_lines:
        if '**most**' not in line:
            fig.text(METH_X, y, line, fontsize=METH_FS,
                     ha='left', family='serif', color=METH_COLOR, zorder=10)
        else:
            left, right = line.split('**most**', 1)
            children = []
            if left:
                children.append(TextArea(left, textprops=dict(
                    fontsize=METH_FS, color=METH_COLOR, family='serif',
                    weight='normal')))
            children.append(TextArea('most', textprops=dict(
                fontsize=METH_FS, color=METH_COLOR, family='serif',
                weight='bold')))
            if right:
                children.append(TextArea(right, textprops=dict(
                    fontsize=METH_FS, color=METH_COLOR, family='serif',
                    weight='normal')))
            packer = HPacker(children=children, align='baseline', pad=0, sep=0)
            ab = AnnotationBbox(packer, (METH_X, y), xycoords='figure fraction',
                                box_alignment=(0, 0.5), frameon=False, pad=0)
            fig.add_artist(ab)
        y -= LINE_SPACING

    # Footer with legend swatch for the no-data hatching.
    from matplotlib.offsetbox import DrawingArea
    from matplotlib.patches import Rectangle
    swatch = DrawingArea(11, 11, 0, 0)
    swatch.add_artist(Rectangle((0, 0), 11, 11,
                                facecolor=NO_DATA_FILL,
                                edgecolor='#9a8f78', linewidth=0.5,
                                hatch='////'))
    legend_text = TextArea(
        ' Insufficient coverage to identify recurring themes  •  '
        'Data from NYT Archive API  •  '
        'Full analysis at tedalcorn.github.io/nyt',
        textprops=dict(fontsize=10, family='serif', color=MUTED,
                       stretch='condensed'))
    footer_packer = HPacker(children=[swatch, legend_text],
                            align='center', pad=0, sep=2)
    footer_ab = AnnotationBbox(footer_packer, (0.98, 0.02),
                               xycoords='figure fraction',
                               box_alignment=(1.0, 0.0),
                               frameon=False, pad=0)
    fig.add_artist(footer_ab)

    out_dir = os.path.join(PROJECT_DIR, 'outputs', '2026-05-top-keyword',
                           '2026-05-13-world-country-tweets', 'Asia-and-Oceania')
    os.makedirs(out_dir, exist_ok=True)
    out_png = os.path.join(out_dir, 'asia-and-oceania-map.png')
    out_pdf = os.path.join(out_dir, 'asia-and-oceania-map.pdf')
    plt.savefig(out_png, dpi=400, facecolor=CREAM)
    plt.savefig(out_pdf, facecolor=CREAM)
    plt.close()
    print(f'  Saved {out_png}')
    print(f'  Saved {out_pdf}')
    if callouts:
        print(f'\nCallouts ({len(callouts)}):')
        for gname, text, _ in callouts:
            print(f'  {gname}: {text}')


if __name__ == '__main__':
    main()
