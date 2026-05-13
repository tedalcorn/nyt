"""Europe map: each European country with sufficient NYT coverage gets its
#1 recurring theme labeled inside the country's polygon — same aesthetic
as the 50-state national map (outputs/top-keyword/-National/state-map.png).

Approach (mirrors build_state_map.py):
  - LAEA Europe projection (EPSG:3035), preserves area
  - Per-country fit-to-polygon labeling: each label tried at multiple font
    sizes / rotations / wrap configurations; the largest that fits inside
    the polygon wins
  - Hand-tuned EUROPE_OVERRIDES for awkward cases (rotations, forced wraps)
  - Callouts placed adjacent to tiny countries with short leader lines
  - Methodology paragraph in an empty area (Atlantic Ocean, west of UK)
  - High-DPI output (400 DPI PNG + SVG + PDF)

Run:
    python3 scripts/build_europe_map.py

Output: outputs/top-keyword/World map/europe-map.{png,svg,pdf}
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

# ── European countries (geojson `NAME` values) ────────────────────────
EUROPEAN_COUNTRIES = {
    'Albania', 'Andorra', 'Austria', 'Belarus', 'Belgium',
    'Bosnia and Herz.', 'Bulgaria', 'Croatia', 'Cyprus', 'Czechia',
    'Denmark', 'Estonia', 'Finland', 'France', 'Germany', 'Greece',
    'Hungary', 'Iceland', 'Ireland', 'Italy', 'Kosovo', 'Latvia',
    'Liechtenstein', 'Lithuania', 'Luxembourg', 'Malta', 'Moldova',
    'Monaco', 'Montenegro', 'Netherlands', 'North Macedonia',
    'Macedonia', 'Norway', 'Poland', 'Portugal', 'Romania', 'Russia',
    'San Marino', 'Serbia', 'Slovakia', 'Slovenia', 'Spain', 'Sweden',
    'Switzerland', 'Turkey', 'Ukraine', 'United Kingdom', 'Vatican',
}

# geojson NAME → analysis country-name (only when they differ)
GEOJSON_TO_ANALYSIS = {
    'United Kingdom': 'Great Britain',
    'Czechia': 'Czech Republic',
    'Bosnia and Herz.': 'Bosnia and Herzegovina',
    'North Macedonia': 'North Macedonia',
}

# Display country name (shorter form used on the map in fine print above
# the theme — disambiguates dense areas like the Balkans).
COUNTRY_DISPLAY_NAME = {
    'United Kingdom': 'UK',
    'Bosnia and Herz.': 'Bosnia',
    'North Macedonia': 'N. Macedonia',
    'Czechia': 'Czechia',
    'Great Britain': 'UK',
}

# Countries small enough that their label needs a tiny country-name caption
# above the theme word (for disambiguation in the dense Balkan / Baltic /
# Central European clusters).
ADD_COUNTRY_CAPTION = {
    'Albania', 'Bosnia and Herz.', 'North Macedonia', 'Macedonia', 'Moldova',
    'Slovakia', 'Slovenia', 'Estonia', 'Latvia', 'Lithuania',
    'Czechia', 'Hungary', 'Austria', 'Bulgaria', 'Romania', 'Belarus',
    'Cyprus', 'Switzerland', 'Belgium', 'Netherlands', 'Denmark',
    'Ireland', 'Portugal',
}

# Per-country fit overrides. Mirrors STATE_OVERRIDES in build_state_map.py.
#   'rotations'    : list of preferred rotations in degrees, tried in order
#   'fs_max'       : font-size cap (overrides global)
#   'fs_min'       : floor
#   'forced_text'  : pre-wrapped string to use instead of the wrap algorithm
#   'anchor_y_frac': 0..1, vertical position within polygon's bbox
#   'fit_threshold': min in-polygon area fraction to accept the fit (0..1)
EUROPE_OVERRIDES = {
    'Norway':     {'rotations': [70, 90, 0]},
    'Sweden':     {'rotations': [70, 90, 0]},
    'Finland':    {'rotations': [80, 90, 0]},
    'Italy':      {'rotations': [-40, -30, 0], 'forced_text': 'Roman\nCivilization'},
    'Portugal':   {'rotations': [70, 90, 0], 'fs_max': 11},
    'Ireland':    {'forced_text': 'Irish-\nAmericans', 'fs_max': 18},
    'United Kingdom': {'forced_text': 'Transit\nSystems', 'fs_max': 26,
                       'anchor_y_frac': 0.25},  # push into wide lower portion
    'Switzerland': {'forced_text': 'Alpine\nSkiing', 'fs_max': 11},
    'Netherlands': {'forced_text': 'Bicycles', 'fs_max': 11},
    'Belgium':    {'forced_text': 'Diamonds', 'fs_max': 11,
                   'rotations': [-30]},  # tilt the other direction per Ted
    # Denmark: rotate nearly vertical so 'Dog Sledding' fits as one
    # continuous line along the country's north-south axis.
    'Denmark':    {'forced_text': 'Dog Sledding', 'fs_max': 13,
                   'rotations': [80]},
    'Croatia':    {'rotations': [-30, 0]},
    'Czechia':    {'forced_text': 'Civil War', 'fs_max': 11},
    # Slovakia is east-west elongated; rotate slightly to match the wide
    # axis and shorten the term so it fits inside the polygon.
    'Slovakia':   {'fs_max': 9, 'rotations': [-10],
                   'forced_text': 'Discrim-\nination', 'tight_caption': True},
    'Slovenia':   {'fs_max': 9, 'forced_text': 'Monuments'},
    'Bosnia and Herz.': {'forced_text': 'War\nCrimes', 'fs_max': 12},
    'North Macedonia': {'fs_max': 8, 'forced_text': 'Dispute over\ncountry\nrenaming'},
    'Macedonia':  {'fs_max': 8, 'forced_text': 'Dispute over\ncountry\nrenaming'},
    'Moldova':    {'fs_max': 10, 'forced_text': 'Secession',
                   'rotations': [-40]},
    'Estonia':    {'fs_max': 9, 'forced_text': 'Memorials'},
    'Latvia':     {'fs_max': 9, 'forced_text': 'Russian\nLanguage'},
    'Lithuania':  {'fs_max': 9, 'forced_text': 'WWII'},
    'Cyprus':     {'fs_max': 8},
    # Albania intentionally not given any text — its #1 'Sociology' is just
    # 3 articles from a single 2008 feature series on Albanian sworn virgins;
    # no meaningful recurring pattern. Country renders no-data fill.
    'Iceland':    {'forced_text': 'Geothermal\nPower', 'fs_max': 11,
                   'anchor_y_frac': 0.35},  # leave room for Iceland: caption above
    'Hungary':    {'forced_text': 'Academic\nFreedom', 'fs_max': 12,
                   'rotations': [-20]},
    'Kosovo':     {'fs_max': 9},
    'Russia':     {'fs_max': 30},
    # Greece's polygon (mainland) is small; lower fit_threshold so 'Greek
    # Civilization' can render at a more visible size even if a few
    # millimeters poke into the Aegean.
    'Greece':     {'rotations': [-30, 0], 'forced_text': 'Greek\nCivilization',
                   'fs_max': 22, 'fit_threshold': 0.80},
    'Serbia':     {'forced_text': 'Chess', 'fs_max': 16},
    'Bulgaria':   {'forced_text': 'Organized\nCrime', 'fs_max': 11},
    'Romania':    {'forced_text': 'Human\nTrafficking', 'fs_max': 12},
    'Poland':     {'forced_text': 'Concentration\nCamps', 'fs_max': 13},
    'Austria':    {'forced_text': 'Skiing', 'fs_max': 13},
    'Belarus':    {'forced_text': 'Voter\nFraud', 'fs_max': 11},
}

# Countries definitely too small even with overrides — use callouts.
# Each entry: (geojson_name, (dx, dy) offset in map-width fractions).
# Iceland is large enough to label in-polygon; microstates without
# enough coverage to score won't even attempt a label.
CALLOUT_OFFSETS = {
    'Andorra':    (-0.04, -0.05),
    'Luxembourg': ( 0.03, -0.01),
    'Liechtenstein': ( 0.03,  0.02),
    'San Marino': ( 0.04,  0.01),
    'Monaco':     (-0.03, -0.01),
    'Malta':      ( 0.03, -0.02),
    'Vatican':    ( 0.04,  0.01),
}

# Bbox in lat/lon — what part of Europe shows. Tuned so the western
# (Atlantic), southern (Mediterranean), and eastern (European Russia)
# borders all sit comfortably inside the frame. Southern edge dropped
# to 33° so Sicily and southern Greek islands aren't cropped.
EUROPE_BBOX_LATLON = (-13, 33, 47, 71)  # minx, miny, maxx, maxy

# Minimum overrepresentation score required for a country to get a label.
# Weak signals (Cyprus's #1 "Palestinians" at 4.4× — a 6-article cluster
# from a single 2002 event) get filtered out and the country renders as
# no-data fill instead.
MIN_SCORE_TO_LABEL = 6.0

# Countries explicitly excluded from labeling even if they pass the score
# threshold — typically because their #1 is a thin one-cluster signal that
# doesn't represent recurring coverage (e.g. Albania's 'Sociology' is just
# 3 articles from a single 2008 feature series).
SKIP_COUNTRIES = {'Albania'}

# Display-name override for forced_text (the override text takes precedence
# over THEME_DISPLAY since we want the forced wrapping)


def get_country_polys(world_gdf, target_crs):
    """Return dict of {geojson_name: list of projected polygons} for
    European countries only, with Russia clipped to the European bulk
    and overseas territories filtered out."""
    name_field = 'NAME' if 'NAME' in world_gdf.columns else 'name'
    out = {}
    for _, row in world_gdf.iterrows():
        name = row[name_field]
        if name not in EUROPEAN_COUNTRIES:
            continue
        geom = row.geometry
        raw = list(geom.geoms) if geom.geom_type == 'MultiPolygon' else [geom]
        if name == 'Russia':
            # Keep only the European bulk (longitudes 27-70)
            raw = [p for p in raw if p.bounds[0] < 70 and p.bounds[0] >= 25]
        if len(raw) > 1:
            biggest = max(raw, key=lambda p: p.area)
            bx0, by0, bx1, by1 = biggest.bounds
            bcx = (bx0 + bx1) / 2
            bcy = (by0 + by1) / 2
            raw = [p for p in raw
                   if abs((p.bounds[0]+p.bounds[2])/2 - bcx) <= 25
                   and abs((p.bounds[1]+p.bounds[3])/2 - bcy) <= 25]
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


def candidate_anchors(poly, anchor_y_frac=None):
    """Yield candidate label-anchor points within poly."""
    pts = [poly.representative_point(), poly.centroid]
    minx, miny, maxx, maxy = poly.bounds
    if anchor_y_frac is not None:
        cy = miny + (maxy - miny) * anchor_y_frac
        cx = (minx + maxx) / 2
        candidate = Point(cx, cy)
        if poly.contains(candidate):
            pts.insert(0, candidate)
    # Along principal axis
    try:
        mrr = poly.convex_hull.minimum_rotated_rectangle
        coords = list(mrr.exterior.coords)
        edges = [(coords[i], coords[i+1]) for i in range(4)]
        longest = max(edges, key=lambda e: math.hypot(e[1][0]-e[0][0], e[1][1]-e[0][1]))
        dx = longest[1][0] - longest[0][0]
        dy = longest[1][1] - longest[0][1]
        angle = math.atan2(dy, dx)
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
    """Try every (font_size, rotation, wrap) combo. Return (cx, cy, fs,
    rotation, text) of the best fit, or None if nothing fits."""
    fs_max = override.get('fs_max', 22)
    fs_min = override.get('fs_min', 6)
    rotations = override.get('rotations', [0])
    anchor_y_frac = override.get('anchor_y_frac')
    fit_threshold = override.get('fit_threshold', 0.97)
    forced_text = override.get('forced_text')

    anchors = list(candidate_anchors(poly, anchor_y_frac=anchor_y_frac))
    if not anchors:
        return None

    # Build the list of (n_lines, text) options
    if forced_text:
        options = [(forced_text.count('\n') + 1, forced_text)]
    else:
        options = list(wrap_options(label, max_lines=4))

    best = None  # (fs, rot_idx, score, ...)
    for fs in range(fs_max, fs_min - 1, -1):
        for n_lines, text in options:
            w, h = measure_text_size(ax, fig, text, fs)
            for rot_idx, rotation in enumerate(rotations):
                for anchor in anchors:
                    rect = rotated_text_box(anchor.x, anchor.y, w, h, rotation)
                    score = text_fit_score(rect, poly)
                    if score >= fit_threshold:
                        return (anchor.x, anchor.y, fs, rotation, text)
                    # Track best near-miss for fallback
                    if best is None or score > best[0]:
                        best = (score, anchor.x, anchor.y, fs, rotation, text)
    # If we didn't find a perfect fit, return the best near-miss only if
    # it's reasonably close. Otherwise let caller fall back to callout.
    if best and best[0] >= 0.85 and best[3] >= fs_min:
        return (best[1], best[2], best[3], best[4], best[5])
    return None


def main():
    print('Loading country geometries…')
    geo_path = os.path.join(PROJECT_DIR, 'data', 'world_countries.geojson')
    world_gdf = gpd.read_file(geo_path)
    target_crs = 'EPSG:3035'

    print('Running theme analysis…')
    arts = load_world_articles()
    res = analyze(arts)
    print(f'  {len(res)} countries scored')

    european_polys = get_country_polys(world_gdf, target_crs)
    print(f'  {len(european_polys)} European country geometries')

    # ── Figure ─────────────────────────────────────────────────────────
    # Compute Europe bbox in projection coords
    bbox_pts = gpd.GeoSeries([
        Point(EUROPE_BBOX_LATLON[0], EUROPE_BBOX_LATLON[1]),
        Point(EUROPE_BBOX_LATLON[2], EUROPE_BBOX_LATLON[1]),
        Point(EUROPE_BBOX_LATLON[0], EUROPE_BBOX_LATLON[3]),
        Point(EUROPE_BBOX_LATLON[2], EUROPE_BBOX_LATLON[3]),
    ], crs=world_gdf.crs).to_crs(target_crs)
    bxs = [p.x for p in bbox_pts]
    bys = [p.y for p in bbox_pts]
    bbox_minx, bbox_maxx = min(bxs), max(bxs)
    bbox_miny, bbox_maxy = min(bys), max(bys)
    eur_w = bbox_maxx - bbox_minx
    eur_h = bbox_maxy - bbox_miny
    bbox_aspect = eur_w / eur_h

    map_w_inches = 18
    map_h_inches = map_w_inches / bbox_aspect
    fig_w = map_w_inches
    fig_h = map_h_inches + 2.0   # 1.4 top for title, 0.6 bottom for footer
    fig = plt.figure(figsize=(fig_w, fig_h), dpi=120, facecolor=CREAM)

    map_ax = fig.add_axes([0.0, 0.6 / fig_h, 1.0, map_h_inches / fig_h])
    map_ax.set_facecolor(CREAM)
    map_ax.set_aspect('equal')
    map_ax.axis('off')
    map_ax.set_xlim(bbox_minx, bbox_maxx)
    map_ax.set_ylim(bbox_miny, bbox_maxy)

    # Title — top-left, like the state map
    title_y = 1.0 - 0.45 / fig_h
    fig.text(0.02, title_y,
             "How The New York Times Looks At Europe",
             fontsize=28, family='serif', weight='semibold',
             color=INK, ha='left', va='top')
    fig.text(0.02, title_y - 0.50 / fig_h,
             "Keywords that The New York Times assigns to its articles "
             "show which recurring subjects are covered in each country "
             "out of proportion to international coverage as a whole.",
             fontsize=12, family='serif', color='#4a4438',
             ha='left', va='top')

    # Draw all countries
    for gname, polys in european_polys.items():
        analysis_name = GEOJSON_TO_ANALYSIS.get(gname, gname)
        has_data = analysis_name in res
        fill = COUNTRY_FILL if has_data else NO_DATA_FILL

        # Drop shadow
        for poly in polys:
            if not poly.exterior:
                continue
            xs, ys = poly.exterior.xy
            rng_x = max(xs) - min(xs)
            rng_y = max(ys) - min(ys)
            ox = rng_x * 0.004
            oy = rng_y * 0.004
            map_ax.fill([x + ox for x in xs], [y - oy for y in ys],
                        facecolor='#c2b9a3', edgecolor='none',
                        alpha=0.30, zorder=1)
        # Fill
        for poly in polys:
            if not poly.exterior: continue
            xs, ys = poly.exterior.xy
            map_ax.fill(xs, ys, facecolor=fill, edgecolor='none', zorder=1.5)
        # Outline
        for poly in polys:
            if not poly.exterior: continue
            xs, ys = poly.exterior.xy
            map_ax.plot(xs, ys, color=BORDER, linewidth=0.7,
                        solid_joinstyle='round', zorder=3)

    # ── Labels ─────────────────────────────────────────────────────────
    callouts = []  # (geojson_name, theme_text, anchor_point)
    for gname, polys in european_polys.items():
        analysis_name = GEOJSON_TO_ANALYSIS.get(gname, gname)
        if analysis_name not in res:
            continue
        if analysis_name in SKIP_COUNTRIES:
            continue
        country_res = res[analysis_name]
        recurring = country_res.get('recurring', [])
        if not recurring:
            continue
        recurring = _condense_olympics(recurring, country_res.get('tag_years', {}))
        top = recurring[0]
        # Drop countries whose strongest signal is below the threshold —
        # those labels would mislead more than inform (e.g. Cyprus
        # "Palestinians" at score 4.4 is a 6-article one-event cluster).
        if top['score'] < MIN_SCORE_TO_LABEL:
            continue
        label = display_name(top['tag'], analysis_name)

        # Clip polygons to visible bbox so labels stay inside the map area
        bbox_rect = ShpPolygon([
            (bbox_minx, bbox_miny), (bbox_maxx, bbox_miny),
            (bbox_maxx, bbox_maxy), (bbox_minx, bbox_maxy),
        ])
        clipped_pieces = []
        for p in polys:
            try:
                c = p.intersection(bbox_rect)
            except Exception:
                continue
            if c.is_empty:
                continue
            if c.geom_type == 'Polygon':
                clipped_pieces.append(c)
            elif c.geom_type == 'MultiPolygon':
                clipped_pieces.extend(c.geoms)
        if not clipped_pieces:
            continue
        biggest = max(clipped_pieces, key=lambda p: p.area)

        # If this country is in the callout-only list, skip in-poly fit
        if gname in CALLOUT_OFFSETS:
            callouts.append((gname, label, biggest.representative_point()))
            continue

        override = EUROPE_OVERRIDES.get(gname, {})
        fit = fit_label(map_ax, fig, biggest, label, override)
        if fit is None:
            callouts.append((gname, label, biggest.representative_point()))
            continue
        cx, cy, fs, rotation, text = fit

        # Country name caption above the topic — applied for every labeled
        # country, in "Name:" format, in lighter ink. Helps readers identify
        # which country each label belongs to (especially in dense clusters).
        # When the topic is rotated, the caption rotates with it.
        country_caption = COUNTRY_DISPLAY_NAME.get(gname, gname) + ':'
        _, h = measure_text_size(map_ax, fig, text, fs)
        cap_fs = max(8, int(fs * 0.62))

        # Offset gap factor — tighter for small countries where vertical
        # space is at a premium (Slovakia, etc.).
        tight = override.get('tight_caption', False)
        gap_factor = 0.50 if tight else 0.65

        # Compute caption position relative to the topic's center, taking
        # rotation into account so the caption sits "above" the topic in
        # the rotated frame.
        rot_rad = math.radians(rotation)
        offset = h * gap_factor
        # Rotation matrix: when text is rotated, "up" in local coords maps to
        # (-sin(rot), cos(rot)) in data coords
        offset_x = -math.sin(rot_rad) * offset
        offset_y = math.cos(rot_rad) * offset
        cap_x = cx + offset_x
        cap_y = cy + offset_y
        map_ax.text(cap_x, cap_y, country_caption,
                    ha='center', va='center',
                    fontsize=cap_fs, family='serif', weight='normal',
                    color=MUTED, rotation=rotation, zorder=4)

        map_ax.text(cx, cy, text,
                    ha='center', va='center',
                    fontsize=fs, family='serif', weight='semibold',
                    color=INK, rotation=rotation, zorder=4)

    # ── Callouts ──────────────────────────────────────────────────────
    # Adjacent placement — each callout gets a short leader line to a
    # nearby off-polygon label location.
    for gname, text, anchor in callouts:
        dx, dy = CALLOUT_OFFSETS.get(gname, (0.04, 0.02))
        lx = anchor.x + dx * eur_w
        ly = anchor.y + dy * eur_h
        map_ax.plot([anchor.x, lx - eur_w * 0.003],
                    [anchor.y, ly],
                    color=LEADER, linewidth=0.7, alpha=0.85, zorder=2.5)
        analysis_name = GEOJSON_TO_ANALYSIS.get(gname, gname)
        map_ax.text(lx, ly + eur_h * 0.012, analysis_name,
                    fontsize=8, ha='left', va='center',
                    family='serif', color=MUTED, zorder=4)
        map_ax.text(lx, ly - eur_h * 0.005, text,
                    fontsize=10, ha='left', va='center',
                    family='serif', weight='semibold', color=INK, zorder=4)

    # ── Methodology paragraph ─────────────────────────────────────────
    # Placed in the upper-left over the Atlantic Ocean (NW of Ireland,
    # south of Iceland) — empty water with no countries to crowd.
    # Matches the state-map approach with **most** inline-bold.
    n_world_articles = sum(1 for a in arts if (a.get('s') or '') == 'World')
    # Round to nearest 1,000 for the methodology copy
    rounded_articles = f"{round(n_world_articles, -3):,.0f}"

    from matplotlib.offsetbox import HPacker, TextArea, AnnotationBbox
    METH_COLOR = '#4a4438'
    # Methodology — Ted's edited text. Slightly wider lines (extends ~3%
    # further right than v8 per Ted's review) so the block fits in fewer
    # lines, and tighter line spacing so it doesn't bleed into Britain.
    methodology_lines = [
        f'This map draws on {rounded_articles} articles in the World section from 2000 to 2026. The',
        'New York Times assigns each article subject keywords (separate from tags for',
        'individual people and organizations, which are not included here). For each',
        'country with sufficient coverage to identify recurring patterns, the map shows',
        'the keyword that (a) appeared on at least 1% of the country’s coverage and',
        '(b) was **most** out of proportion with that keyword’s frequency in World',
        'coverage overall. The analysis excludes each country’s own name and currency,',
        'broad topics applied to most countries such as “international relations,” and',
        'one-time events such as named storms, major accidents, and specific Olympic Games.',
    ]
    METH_X = 0.025
    METH_FS = 9
    LINE_SPACING = 0.012   # ~33% tighter than the previous hardcoded 0.018
    y = 0.710
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

    # ── Footer ─────────────────────────────────────────────────────────
    # (No "insufficient coverage" list — methodology text already explains
    # that countries with insufficient coverage are unlabeled.)
    fig.text(0.98, 0.02,
             'Data from NYT Archive API  •  Full analysis at tedalcorn.github.io/nyt',
             fontsize=11, ha='right', family='serif', color=MUTED, zorder=10)

    # ── Save ───────────────────────────────────────────────────────────
    out_dir = os.path.join(PROJECT_DIR, 'outputs', 'top-keyword', 'World map')
    os.makedirs(out_dir, exist_ok=True)
    out_png = os.path.join(out_dir, 'europe-map.png')
    out_svg = os.path.join(out_dir, 'europe-map.svg')
    out_pdf = os.path.join(out_dir, 'europe-map.pdf')
    plt.savefig(out_png, dpi=400, facecolor=CREAM)
    plt.savefig(out_svg, facecolor=CREAM)
    plt.savefig(out_pdf, facecolor=CREAM)
    plt.close()
    print(f'  Saved {out_png}')
    print(f'  Saved {out_svg}')
    print(f'  Saved {out_pdf}')

    if callouts:
        print(f'\nCallouts ({len(callouts)}):')
        for gname, text, _ in callouts:
            print(f'  {gname}: {text}')


if __name__ == '__main__':
    main()
