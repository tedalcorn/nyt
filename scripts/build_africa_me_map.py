"""Africa + Middle East regional map: each country with sufficient NYT
coverage gets its #1 recurring theme labeled inside its polygon.

Same aesthetic and label-fit algorithm as the Europe and Americas maps.
Saudi Arabia in this region has the 9/11 carve-out applied (Pentagon,
WTC, Hijacking, Airlines stripped from its scoring); the methodology
notes that briefly.

Run:
    python3 scripts/build_africa_me_map.py

Output: outputs/2026-05-top-keyword/2026-05-13-world-country-tweets/
        Africa-and-Middle-East/africa-and-middle-east-map.{png,pdf}
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
AFME_COUNTRIES = {
    # Africa
    'Algeria','Angola','Benin','Botswana','Burkina Faso','Burundi','Cameroon',
    'Central African Rep.','Chad','Congo','Côte d\'Ivoire','Dem. Rep. Congo',
    'Djibouti','Egypt','Eq. Guinea','Eritrea','eSwatini','Ethiopia','Gabon',
    'Gambia','Ghana','Guinea','Guinea-Bissau','Kenya','Lesotho','Liberia',
    'Libya','Madagascar','Malawi','Mali','Mauritania','Morocco','Mozambique',
    'Namibia','Niger','Nigeria','Rwanda','Senegal','Sierra Leone','Somalia',
    'Somaliland','South Africa','S. Sudan','Sudan','Tanzania','Togo','Tunisia',
    'Uganda','W. Sahara','Zambia','Zimbabwe',
    # Middle East
    'Bahrain','Iran','Iraq','Israel','Jordan','Kuwait','Lebanon','Oman',
    'Qatar','Saudi Arabia','Syria','United Arab Emirates','Yemen',
}

# geojson NAME → analysis country-name (only when they differ)
GEOJSON_TO_ANALYSIS = {
    'Dem. Rep. Congo': 'Democratic Republic of Congo',
    'Congo': 'Republic of Congo',
    'Eq. Guinea': 'Equatorial Guinea',
    'S. Sudan': 'South Sudan',
    'Côte d\'Ivoire': 'Ivory Coast',
    'Central African Rep.': 'Central African Republic',
    'eSwatini': 'Eswatini',
    'W. Sahara': 'Western Sahara',
}

# Short display names for the country caption
COUNTRY_DISPLAY_NAME = {
    'Dem. Rep. Congo': 'DRC',
    'Central African Rep.': 'C.A.R.',
    'United Arab Emirates': 'UAE',
    'Côte d\'Ivoire': 'Ivory Coast',
    'Eq. Guinea': 'Eq. Guinea',
    'S. Sudan': 'S. Sudan',
    'eSwatini': 'Eswatini',
    'Saudi Arabia': 'Saudi Arabia',
}

# Per-country fit overrides.
AFME_OVERRIDES = {
    # Big African countries: large fs and varied rotations per country's
    # widest dimension. fit_label tries each rotation and picks the
    # largest font that fits.
    'Egypt':              {'forced_text': 'Egyptian\nCivilization', 'fs_max': 36,
                           'anchor_y_frac': 0.45,
                           'rotations': [0]},
    'South Africa':       {'forced_text': 'Apartheid', 'fs_max': 48,
                           'rotations': [30, 0],
                           'anchor_y_frac': 0.65},  # bias north to dodge Lesotho hole
    'Algeria':            {'forced_text': 'Berbers', 'fs_max': 46,
                           'rotations': [-15, 0]},
    'Libya':              {'forced_text': 'Diplomatic\nEmbassies', 'fs_max': 28,
                           'rotations': [-10, 0]},
    'Sudan':              {'forced_text': 'Tribes and\nTribalism', 'fs_max': 30,
                           'rotations': [0]},
    'S. Sudan':           {'forced_text': 'Gold', 'fs_max': 22,
                           'anchor_y_frac': 0.42},  # nudge down so label fits inside borders
    'Ethiopia':           {'forced_text': 'Oromo', 'fs_max': 30,
                           'rotations': [-30, 0]},
    'Dem. Rep. Congo':    {'forced_text': 'Cobalt', 'fs_max': 42,
                           'rotations': [0]},
    'Tanzania':           {'forced_text': 'US Foreign\nService', 'fs_max': 18,
                           'rotations': [30, 0]},
    'Nigeria':            {'forced_text': 'Poliomyelitis', 'fs_max': 28,
                           'rotations': [30, 0]},
    'Iran':               {'forced_text': 'Iran-Israel\nProxy Conflict', 'fs_max': 26,
                           'rotations': [-25, 0]},
    'Iraq':               {'forced_text': 'Yazidi', 'fs_max': 30,
                           'fit_threshold': 0.85},
    'Saudi Arabia':       {'forced_text': 'Pilgrimages', 'fs_max': 44,
                           'rotations': [-25, 0]},
    'Yemen':              {'forced_text': 'Navies', 'fs_max': 26,
                           'rotations': [0]},
    # Morocco → callout (offshore in the Atlantic, NW of country)
    'Tunisia':            {'forced_text': 'Drownings', 'fs_max': 11,
                           'rotations': [80, 0]},
    'Mali':               {'forced_text': 'Tuareg', 'fs_max': 32,
                           'rotations': [-15, 0]},
    'Niger':              {'forced_text': 'Poliomyelitis', 'fs_max': 22,
                           'rotations': [0]},
    'Chad':               {'forced_text': 'Pipelines', 'fs_max': 18,
                           'rotations': [80, 0]},
    'Cameroon':           {'forced_text': 'Medicine\nand Health', 'fs_max': 12,
                           'rotations': [40, 0],
                           'anchor_x_frac': 0.42, 'anchor_y_frac': 0.35},
    'Kenya':              {'forced_text': 'Kikuyu', 'fs_max': 20,
                           'rotations': [-30, 0]},
    'Uganda':             {'forced_text': 'Condoms', 'fs_max': 11},
    'Rwanda':             {'forced_text': 'Conflict\nMinerals', 'fs_max': 8},
    'Burundi':            {'forced_text': 'Hutu', 'fs_max': 9},
    'Angola':             {'forced_text': 'Diamonds', 'fs_max': 28,
                           'rotations': [0]},
    'Mozambique':         {'forced_text': 'Cyclones', 'fs_max': 18,
                           'rotations': [-60, 0]},
    'Madagascar':         {'fs_max': 14, 'rotations': [-65, 0]},
    'Zambia':             {'forced_text': 'Diet and\nNutrition', 'fs_max': 18,
                           'rotations': [-20, 0],
                           'anchor_x_frac': 0.50,
                           'anchor_y_frac': 0.30},  # lower in country to clear Angola/Diamonds
    'Zimbabwe':           {'forced_text': 'Lions', 'fs_max': 20},
    'Malawi':             {'forced_text': 'Diet and\nNutrition', 'fs_max': 6,
                           'rotations': [80, 0]},
    'Eritrea':            {'forced_text': 'Tigrayans', 'fs_max': 9,
                           'rotations': [80, 0]},
    'Liberia':            {'forced_text': 'Ebola', 'fs_max': 11},
    'Sierra Leone':       {'forced_text': 'Ebola', 'fs_max': 9},
    'Guinea':             {'forced_text': 'Ebola', 'fs_max': 11,
                           'rotations': [-30, 0]},
    'Ghana':              {'forced_text': 'Malaria', 'fs_max': 11,
                           'rotations': [80, 0]},
    'Côte d\'Ivoire':     {'forced_text': 'Civil War', 'fs_max': 13},
    'Burkina Faso':       {'forced_text': 'Mercenaries', 'fs_max': 9},
    'Senegal':            {'forced_text': 'Sufism', 'fs_max': 12},
    'Central African Rep.': {'forced_text': 'Mercenaries', 'fs_max': 18,
                           'rotations': [0]},
    'Somalia':            {'forced_text': 'Piracy', 'fs_max': 26,
                           'rotations': [45]},
    # Jordan → callout (NW of country, into open space above Israel)
    'Eswatini':           {'forced_text': 'AIDS', 'fs_max': 7},
    # Israel / Lebanon / Syria moved to callouts (see below)
}

# Small countries → callouts
# (dx, dy[, rotation[, forced_text[, ha_override]]])
CALLOUT_OFFSETS = {
    # Northern Middle East cluster — all annotated FROM OUTSIDE (mostly
    # north) so the tangle of small countries can each be readable.
    'Israel':      (-0.030,  0.040, 0, 'Temple Mount', 'center'),
    'Lebanon':     (-0.025,  0.080, 0, 'Iran-Israel\nProxy Conflict', 'center'),
    'Syria':       ( 0.025,  0.090, 0, 'Assyrian Civilization', 'center'),
    # Jordan: Temple Mount is also Jordan's #1 — the Hashemite kingdom
    # is the official Muslim custodian of the site, so the duplication
    # with Israel reflects real NYT coverage. Showing as-is.
    'Jordan':      (-0.055,  0.020, 0, 'Temple Mount', 'center'),
    # Kuwait nudged a tiny bit more up
    'Kuwait':      ( 0.020,  0.125, 0, 'Persian Gulf War', 'center'),
    # Persian Gulf small states — Bahrain pushed far right (~near right
    # edge of image) and ~2 lines down. ha='right' so the text ENDS at
    # the anchor, keeping it on-canvas.
    'Bahrain':     ( 0.165, -0.012, 0, 'Tear Gas', 'right'),
    # Qatar pushed far south into open Gulf space, center-aligned
    'Qatar':       ( 0.060, -0.120, 0, 'Peace Process', 'center'),
    # UAE pushed right and down (~2.5 text lines)
    'United Arab Emirates': ( 0.075, -0.087, 0,
                             'High Net Worth\nIndividuals', 'center'),
    # Morocco: callout offshore in the Atlantic, NW of country
    'Morocco':     (-0.035,  0.020, 30, 'Railroads', 'center'),
    # Rwanda: callout placed left + up so it sits inside DRC's empty
    # space (DRC's text 'Cobalt' is further south on the polygon).
    # Heavily wrapped so it stacks vertically in the small space.
    'Rwanda':      (-0.040,  0.030, 0,
                    'Conflict\nMinerals\n& Resources', 'center'),
    # African micro states
    'Djibouti':    (0.022, -0.000),
    'Gambia':      (-0.020, 0.000),
    'Lesotho':     (0.022, -0.005),
    'eSwatini':    (0.022, -0.005, 0, 'AIDS'),
}

# Skip-countries set — special exclusions
SKIP_COUNTRIES = set()

# Bbox in lat/lon — covers all of Africa (lat -35 to 38) and the Middle
# East (lon ~25-60). Slightly wider west (-20) to give Senegal/Mauritania
# room, east (60) for Iran's eastern border.
AFME_BBOX_LATLON = (-26, -38, 72, 40)

MIN_SCORE_TO_LABEL = 6.0
MIN_TAG_YEAR_SPAN = 2


def pick_afme_projection():
    """Lambert Azimuthal Equal-Area centered on the Africa+ME bounds."""
    return '+proj=laea +lat_0=5 +lon_0=20 +datum=WGS84 +units=m +no_defs'


def get_country_polys(world_gdf, target_crs):
    name_field = 'NAME' if 'NAME' in world_gdf.columns else 'name'
    out = {}
    for _, row in world_gdf.iterrows():
        name = row[name_field]
        if name not in AFME_COUNTRIES:
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
    target_crs = pick_afme_projection()

    print('Running theme analysis…')
    arts = load_world_articles()
    res = analyze(arts)
    print(f'  {len(res)} countries scored')

    polys_by_country = get_country_polys(world_gdf, target_crs)
    print(f'  {len(polys_by_country)} country geometries')

    bbox_pts = gpd.GeoSeries([
        Point(AFME_BBOX_LATLON[0], AFME_BBOX_LATLON[1]),
        Point(AFME_BBOX_LATLON[2], AFME_BBOX_LATLON[1]),
        Point(AFME_BBOX_LATLON[0], AFME_BBOX_LATLON[3]),
        Point(AFME_BBOX_LATLON[2], AFME_BBOX_LATLON[3]),
    ], crs=world_gdf.crs).to_crs(target_crs)
    bxs = [p.x for p in bbox_pts]
    bys = [p.y for p in bbox_pts]
    bbox_minx, bbox_maxx = min(bxs), max(bxs)
    bbox_miny, bbox_maxy = min(bys), max(bys)
    eur_w = bbox_maxx - bbox_minx
    eur_h = bbox_maxy - bbox_miny
    bbox_aspect = eur_w / eur_h

    # Africa+ME is nearly square (~1.05:1 wide). Layout: top margin for
    # title + subtitle + 9/11 note (~5 subtitle lines), bottom margin
    # for footer.
    map_h_inches = 16
    map_w_inches = map_h_inches * bbox_aspect
    TOP_MARGIN_INCHES = 3.2   # title (~1.0") + 5-line subtitle at 19pt (~2.0") + buffer
                              # (was 3.6; cropped 0.4" off the top — undo: 3.6)
    BOTTOM_MARGIN_INCHES = 0.4
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

    # Title on ONE line spanning the figure, subtitle on 2 wider lines.
    # Both lowered ~1 big line for more air at the top.
    title_y = 1.0 - 0.55 / fig_h
    SUB_LINE_INCH = 0.35
    sub_y1 = title_y - 0.70 / fig_h
    sub_y2 = sub_y1 - SUB_LINE_INCH / fig_h
    title_block_bottom_y = sub_y2 - 0.15 / fig_h

    fig.text(0.02, title_y, "How The New York Times Looks At Africa & the Middle East",
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
            callouts.append((gname, label, biggest.representative_point()))
            continue

        override = AFME_OVERRIDES.get(gname, {})
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

    # Bahrain is too tiny to be in this geojson — inject a synthetic
    # anchor at its actual lat/lon so the callout still renders.
    bahrain_pt = gpd.GeoSeries(
        [Point(50.55, 26.0)], crs=world_gdf.crs).to_crs(target_crs)[0]
    callouts.append(('Bahrain', 'Tear Gas', bahrain_pt))

    # Callouts with tight name/term stacking + shortened leaders
    def _text_height_data(fs, n_lines=1):
        inv = map_ax.transData.inverted()
        h_pt = fs * 1.18 * n_lines
        h_px = h_pt * fig.dpi / 72
        (_, y0) = inv.transform((0, 0))
        (_, y1) = inv.transform((0, h_px))
        return abs(y1 - y0)

    NAME_FS = 8
    TERM_FS = 10
    for gname, text, anchor in callouts:
        cfg = CALLOUT_OFFSETS.get(gname, (0.04, 0.02))
        dx, dy = cfg[0], cfg[1]
        rotation = cfg[2] if len(cfg) > 2 else 0
        if len(cfg) > 3 and cfg[3]:
            text = cfg[3]
        ha_override = cfg[4] if len(cfg) > 4 else None
        lx = anchor.x + dx * eur_w
        ly = anchor.y + dy * eur_h

        leader_end_x = anchor.x + 0.85 * (lx - anchor.x)
        leader_end_y = anchor.y + 0.85 * (ly - anchor.y)
        map_ax.plot([anchor.x, leader_end_x], [anchor.y, leader_end_y],
                    color=LEADER, linewidth=0.7, alpha=0.85, zorder=2.5)

        analysis_name = GEOJSON_TO_ANALYSIS.get(gname, gname)
        ha = ha_override if ha_override else ('right' if dx < 0 else 'left')

        n_term_lines = text.count('\n') + 1
        name_h = _text_height_data(NAME_FS, 1)
        term_h = _text_height_data(TERM_FS, n_term_lines)
        between = (name_h + term_h) / 2
        rot_rad = math.radians(rotation)
        name_x = lx + (-math.sin(rot_rad)) * between
        name_y = ly + math.cos(rot_rad) * between

        map_ax.text(lx, ly, text,
                    fontsize=TERM_FS, ha=ha, va='center',
                    family='serif', weight='semibold', color=INK,
                    rotation=rotation, zorder=4)
        map_ax.text(name_x, name_y, analysis_name + ':',
                    fontsize=NAME_FS, ha=ha, va='center',
                    family='serif', color=MUTED, rotation=rotation, zorder=4)

    # Methodology — placed in the open Atlantic Ocean off the West African
    # coast (lower-left of map). Includes the 9/11 carve-out note since
    # Saudi Arabia is in this region.
    n_world_articles = sum(1 for a in arts if (a.get('s') or '') == 'World')
    rounded_articles = f"{round(n_world_articles, -3):,.0f}"
    from matplotlib.offsetbox import HPacker, TextArea, AnnotationBbox
    METH_COLOR = '#4a4438'
    # Methodology — widened lines (~25% more chars per line) and start
    # 3 lines lower than v1 per Ted's note.
    methodology_lines = [
        f'This map draws on {rounded_articles} articles in the',
        'World section from 2000 to 2026. The New',
        'York Times assigns each article subject',
        'keywords (separate from tags for individual',
        'people and organizations, which are not',
        'included here). For each country with',
        'sufficient coverage to identify recurring',
        'patterns, the map shows the keyword that',
        '(a) appeared on at least 1% of the country’s',
        'coverage and (b) was **most** out of',
        'proportion with that keyword’s frequency in',
        'World coverage overall. The analysis',
        'excludes each country’s own currency and',
        'majority ethnic group, broad topics applied',
        'to most countries such as “international',
        'relations,” and one-time events such as named',
        'storms, major accidents, and specific Olympic',
        'Games. In Saudi Arabia, tags related to 9/11',
        'that dominated coverage in the early 2000s',
        'are also excluded so more typical recurring',
        'themes can surface.',
    ]
    METH_X = 0.025
    METH_FS = 13
    LINE_SPACING = 0.016
    # Position: lower-left, in the open Atlantic Ocean off W. Africa.
    # Bumped font 11→13pt, ~3px more right margin, ~23 lines. Starts
    # at roughly equatorial latitude in the projection so it sits in
    # the wider Atlantic strip south of the W. African coast.
    y = 0.41
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

    # Footer with legend swatch for no-data hatching.
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
                           '2026-05-13-world-country-tweets', 'Africa-and-Middle-East')
    os.makedirs(out_dir, exist_ok=True)
    out_png = os.path.join(out_dir, 'africa-and-middle-east-map.png')
    out_pdf = os.path.join(out_dir, 'africa-and-middle-east-map.pdf')
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
