"""Master 50-state map: each state filled with the text of its #1 recurring
theme, sized and wrapped to fit inside the state's borders.

Output: outputs/top-keyword/2026-05-12-us-state-tweets/-National/state-map.{png,svg,pdf}

Approach:
  - Albers Equal-Area projection for CONUS, native scale
  - AK + HI projected to their own Albers, scaled smaller and placed below
    CONUS in clear inset positions (no overlap)
  - Per-state fit-to-polygon labeling: each label tried at multiple font
    sizes and wrap configurations; the largest that fits inside the polygon
    wins. Fallback callouts only for states genuinely too small for any
    legible text (D.C., Rhode Island, sometimes Delaware).
"""

import os
import sys
import math
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.affinity import scale as shp_scale, translate as shp_translate
from shapely.geometry import box as shp_box, Polygon as ShpPolygon, Point

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_DIR, 'scripts'))
from build_state_keywords import load_articles, analyze
from build_state_cards import THEME_DISPLAY, display_name

CREAM = '#f4efe6'
INK = '#2a2a2a'
MUTED = '#7a7368'
TITLE_BLUE = '#326891'
STATE_FILL = '#ece5d4'
STATE_EDGE = '#3a3530'
LEADER = '#9a948a'

# States too small for any in-polygon label even at minimum font; use callout.
HARD_CALLOUT = {'D.C.', 'Rhode Island', 'Maryland'}

# Per-state overrides for the master map. Each entry can specify:
#   'rotations'   : list of preferred rotations (in degrees), tried in order
#   'fs_max'      : font-size cap for this state (overrides global)
#   'fs_min'      : floor (overrides global)
#   'forced_text' : a pre-wrapped string to use instead of the wrap algorithm
#   'anchor_y_frac' : 0..1, vertical position within the polygon's bbox
#                     (0 = bottom, 1 = top, 0.5 = centroid). Used to push
#                     a label into a wider sub-region of an irregular state.
STATE_OVERRIDES = {
    'Indiana':       {'rotations': [90, 0]},
    'Arkansas':      {'rotations': [45, -45, 0]},
    'Vermont':       {'rotations': [90, 0], 'fs_min': 4},
    'New Hampshire': {'rotations': [90, 0], 'fs_min': 4},
    'Florida':       {'rotations': [-35, 0], 'forced_text': 'Cuban-\nAmericans'},
    'California':    {'rotations': [-35, -30, 0]},   # 5° more rotation
    'Michigan':      {'rotations': [90, -10, 0], 'anchor_y_frac': 0.30,
                      'forced_text': 'Arab-\nAmericans'},
    # MN: wrap at the hyphen, force horizontal so a bigger font fits
    'Minnesota':     {'forced_text': 'Somali-\nAmericans',
                      'rotations': [0], 'strict_rotation': True,
                      'fs_max': 32},
    # NY: angled, anchored low; cap font so it stays inside the borders.
    'New York':      {'rotations': [-2, -5, -8],
                      'strict_rotation': True, 'anchor_y_frac': 0.20,
                      'fs_max': 22, 'fs_min': 14, 'fit_threshold': 0.92},
    'New Jersey':    {'rotations': [90, 0], 'fs_min': 14, 'fit_threshold': 0.65},
    'Colorado':      {'fs_max': 30},
    'Ohio':          {'fs_max': 30},
    'Louisiana':     {'anchor_y_frac': 0.25},
    # KY: lower anchor + bigger font for Bourbon
    'Kentucky':      {'anchor_y_frac': 0.30, 'fs_max': 44},
    # ID: lower anchor lets Wolves run wider in the broader south of state
    'Idaho':         {'anchor_y_frac': 0.30, 'fs_max': 56},
    'Pennsylvania':  {'fs_max': 24},
    # AZ / NM: bigger Navajo Indians (was over-shrunk)
    'Arizona':       {'fs_max': 30},
    'New Mexico':    {'fs_max': 30},
    'Oregon':        {'fs_max': 24},
    'Wyoming':       {'fs_max': 26},
    'Nevada':        {'forced_text': 'Burning\nMan\nFestival'},
    # SC: bigger so 'Flags, Emblems and Insignia' fills more of the state
    'South Carolina': {'fs_max': 30},
    # WV: lower-left anchor + bigger font for 'Mines and Mining'
    'West Virginia': {'anchor_y_frac': 0.30, 'fs_max': 38},
    'Alaska':        {'forced_text': 'Aval-\nanches', 'fs_max': 26},
}


def get_geom(states_gdf, name):
    if name == 'D.C.':
        row = states_gdf[states_gdf['NAME'] == 'District of Columbia']
    else:
        row = states_gdf[states_gdf['NAME'] == name]
    return row.iloc[0].geometry if not row.empty else None


def wrap_options(label, max_lines=4):
    """Yield (n_lines, wrapped_text) options from 1 line up to max_lines.
    Splits words as evenly as possible across lines."""
    words = label.split()
    yield 1, label
    if len(words) <= 1:
        return
    for n in range(2, min(max_lines, len(words)) + 1):
        # Distribute words across n lines
        per = len(words) // n
        extra = len(words) % n
        out, i = [], 0
        for k in range(n):
            count = per + (1 if k < extra else 0)
            out.append(' '.join(words[i:i + count]))
            i += count
        yield n, '\n'.join(out)


def principal_axis_angle(poly):
    """Return the angle (degrees) of the longest edge of the polygon's
    minimum rotated bounding rectangle. This is the orientation of the
    state's longest dimension — what we'd want text to follow.

    Falls back to 0 (horizontal) if the polygon's geometry is degenerate
    enough that minimum_rotated_rectangle fails."""
    try:
        # Operate on the convex hull — more reliable than the raw polygon
        # for irregular MultiPolygon-derived shapes.
        mrr = poly.convex_hull.minimum_rotated_rectangle
        coords = list(mrr.exterior.coords)
        edges = [(coords[i], coords[i + 1]) for i in range(4)]
    except Exception:
        return 0.0
    longest = max(edges,
                  key=lambda e: math.hypot(e[1][0] - e[0][0],
                                           e[1][1] - e[0][1]))
    dx = longest[1][0] - longest[0][0]
    dy = longest[1][1] - longest[0][1]
    angle = math.degrees(math.atan2(dy, dx))
    while angle > 90:
        angle -= 180
    while angle <= -90:
        angle += 180
    return angle


def measure_text_size(ax, fig, text, fs):
    """Estimate text size in data coordinates analytically. The char-width
    ratio is generous (0.68) so the estimate OVERshoots actual rendered
    width, preventing visible overflow."""
    lines = text.split('\n')
    n_lines = len(lines)
    max_chars = max(len(l) for l in lines)
    width_pt = max_chars * fs * 0.68
    height_pt = n_lines * fs * 1.22
    dpi = fig.dpi
    width_px = width_pt * dpi / 72.0
    height_px = height_pt * dpi / 72.0
    inv = ax.transData.inverted()
    (x0, y0) = inv.transform((0, 0))
    (x1, y1) = inv.transform((width_px, height_px))
    return abs(x1 - x0), abs(y1 - y0)


def rotated_text_box(cx, cy, w, h, angle_deg):
    """Return the 4 corners (as a Shapely polygon) of a rectangle of
    width w × height h centered at (cx, cy) and rotated by angle_deg."""
    a = math.radians(angle_deg)
    cos_a, sin_a = math.cos(a), math.sin(a)
    hw, hh = w / 2, h / 2
    corners_local = [(-hw, -hh), (hw, -hh), (hw, hh), (-hw, hh)]
    corners = [(cx + x * cos_a - y * sin_a,
                cy + x * sin_a + y * cos_a) for x, y in corners_local]
    return ShpPolygon(corners)


def best_label_anchor(poly):
    """Return the polygon's representative interior point (always inside)."""
    rp = poly.representative_point()
    if poly.contains(rp):
        return rp.x, rp.y
    c = poly.centroid
    return c.x, c.y


def candidate_anchors(poly, n_extra=4):
    """Yield candidate label-anchor points: representative point, centroid,
    and points along the principal axis. Each is guaranteed inside poly."""
    pts = [poly.representative_point(), poly.centroid]
    angle = principal_axis_angle(poly)
    a_rad = math.radians(angle)
    cx, cy = poly.centroid.x, poly.centroid.y
    minx, miny, maxx, maxy = poly.bounds
    diag = math.hypot(maxx - minx, maxy - miny)
    cos_a, sin_a = math.cos(a_rad), math.sin(a_rad)
    for t in [-0.30, -0.15, 0.15, 0.30][:n_extra]:
        px = cx + t * diag * cos_a
        py = cy + t * diag * sin_a
        pts.append(Point(px, py))
    seen = set()
    for p in pts:
        key = (round(p.x, 2), round(p.y, 2))
        if key in seen:
            continue
        seen.add(key)
        if poly.contains(p):
            yield p


def text_fit_score(rect, poly, interior):
    """Returns 1.0 if rect fully inside interior; otherwise the fraction
    of the rect's area that lies inside the polygon (0..1)."""
    if interior.contains(rect):
        return 1.0
    inter = poly.intersection(rect)
    if inter.is_empty:
        return 0.0
    return inter.area / rect.area


def fit_label(ax, fig, poly, label,
              fs_max=40, fs_min=6, shrink=0.92, max_lines=5,
              fit_threshold=0.97,
              prefer_horizontal_aspect=1.3,
              placed_rects=None,
              override=None):
    """Find the largest font size + rotation + anchor that fits label
    inside poly without overlapping any rectangle in `placed_rects`.

    `override` is an optional dict of per-state preferences (see
    STATE_OVERRIDES) — rotations, fs caps, forced text, custom anchor.
    """
    override = override or {}
    placed_rects = placed_rects or []
    minx, miny, maxx, maxy = poly.bounds
    width = maxx - minx
    height = maxy - miny
    aspect = width / height if height > 0 else 1.0

    # Apply per-state overrides
    fs_max = override.get('fs_max', fs_max)
    fs_min = override.get('fs_min', fs_min)
    fit_threshold = override.get('fit_threshold', fit_threshold)

    margin = min(width, height) * (1 - shrink) / 2
    interior = poly.buffer(-margin) if margin > 0 else poly
    if interior.is_empty:
        interior = poly

    # Rotation list — per-state override takes priority, falls back to
    # auto-determined order. With strict_rotation, the override list is
    # used as-is (no 0° fallback) — useful when we want to force an angle
    # even if the algorithm would otherwise pick a bigger horizontal fit.
    if 'rotations' in override:
        rotations = list(override['rotations'])
        if not override.get('strict_rotation') and 0 not in rotations:
            rotations.append(0)
    else:
        pa_angle = principal_axis_angle(poly)
        rotations = []
        if aspect >= prefer_horizontal_aspect:
            rotations.append(0)
            if abs(pa_angle) > 5:
                rotations.append(pa_angle)
        elif aspect <= 1.0 / prefer_horizontal_aspect:
            rotations.append(pa_angle)
            rotations.append(0)
        else:
            if abs(pa_angle) > 5:
                rotations.append(pa_angle)
            rotations.append(0)

    # Custom anchor: shift centroid vertically within polygon's bbox
    custom_anchor = None
    if 'anchor_y_frac' in override:
        ay = override['anchor_y_frac']
        target_y = miny + ay * (maxy - miny)
        # Find a point in the polygon at that y near the centroid x
        cx = poly.centroid.x
        candidate = Point(cx, target_y)
        if poly.contains(candidate):
            custom_anchor = candidate
        else:
            # Walk left/right until we find an interior point at that y
            for dx in (0.05, -0.05, 0.10, -0.10, 0.20, -0.20, 0.30, -0.30):
                test = Point(cx + dx * width, target_y)
                if poly.contains(test):
                    custom_anchor = test
                    break

    if custom_anchor is not None:
        anchors = [custom_anchor] + list(candidate_anchors(poly))
    else:
        anchors = list(candidate_anchors(poly))

    # Forced wrap (e.g. Florida → "Cuban-\nAmericans")
    forced = override.get('forced_text')

    for fs in range(int(fs_max), int(fs_min) - 1, -1):
        if forced is not None:
            text_options = [(forced.count('\n') + 1, forced)]
        else:
            text_options = list(wrap_options(label, max_lines))
        for n_lines, text in text_options:
            w, h = measure_text_size(ax, fig, text, fs)
            for rotation in rotations:
                for anchor in anchors:
                    rect = rotated_text_box(anchor.x, anchor.y, w, h, rotation)
                    score = text_fit_score(rect, poly, interior)
                    if score < fit_threshold:
                        continue
                    if any(rect.intersects(pr) for pr in placed_rects):
                        continue
                    artist = ax.text(anchor.x, anchor.y, text,
                                     fontsize=fs, rotation=rotation,
                                     ha='center', va='center',
                                     family='serif', weight='semibold',
                                     color=INK, zorder=2,
                                     linespacing=0.95)
                    return artist, fs, rotation, n_lines, rect

    return None


def force_label(ax, poly, label, fs=10, rotation=0):
    """Last-resort label placement: use representative point at given fs."""
    p = poly.representative_point()
    return ax.text(p.x, p.y, label, fontsize=fs, rotation=rotation,
                   ha='center', va='center', family='serif',
                   weight='semibold', color=INK, zorder=2)


def main():
    print('Loading data…')
    arts = load_articles()
    res = analyze(arts)

    states_gdf = gpd.read_file(os.path.join(PROJECT_DIR, 'data', 'us_states.geojson'))

    # ── Filter to actual U.S. states + DC, drop territories ────────────────
    REAL_STATES = {
        'Alabama','Alaska','Arizona','Arkansas','California','Colorado',
        'Connecticut','Delaware','District of Columbia','Florida','Georgia',
        'Hawaii','Idaho','Illinois','Indiana','Iowa','Kansas','Kentucky',
        'Louisiana','Maine','Maryland','Massachusetts','Michigan','Minnesota',
        'Mississippi','Missouri','Montana','Nebraska','Nevada','New Hampshire',
        'New Jersey','New Mexico','New York','North Carolina','North Dakota',
        'Ohio','Oklahoma','Oregon','Pennsylvania','Rhode Island','South Carolina',
        'South Dakota','Tennessee','Texas','Utah','Vermont','Virginia',
        'Washington','West Virginia','Wisconsin','Wyoming',
    }
    states_gdf = states_gdf[states_gdf['NAME'].isin(REAL_STATES)].reset_index(drop=True)

    # ── Project CONUS to Albers Equal-Area Conic (EPSG:5070) ────────────────
    conus = (states_gdf[~states_gdf['NAME'].isin(['Alaska', 'Hawaii'])]
             .to_crs('EPSG:5070'))
    conus_bounds = conus.total_bounds
    conus_w = conus_bounds[2] - conus_bounds[0]
    conus_h = conus_bounds[3] - conus_bounds[1]

    # ── Alaska: prepare polygons for a SEPARATE inset axes in the upper-
    # right of the figure (NOT inside the main map data range). Just
    # collect the cleaned, projected polygons here; positioning happens
    # later when we create the inset.
    ak_geom = get_geom(states_gdf, 'Alaska')
    ak_kept = [p for p in ak_geom.geoms if p.bounds[2] < 0]
    ak_proj_polys = list(gpd.GeoSeries(ak_kept, crs=states_gdf.crs)
                         .to_crs('EPSG:3338'))
    ak_proj_xmin = min(p.bounds[0] for p in ak_proj_polys)
    ak_proj_xmax = max(p.bounds[2] for p in ak_proj_polys)
    ak_proj_ymin = min(p.bounds[1] for p in ak_proj_polys)
    ak_proj_ymax = max(p.bounds[3] for p in ak_proj_polys)

    # ── Hawaii: also a SEPARATE inset (in the figure's bottom-left,
    # aligned with the title's left margin). Polygons stay in their
    # native projection; positioning happens via figure coords later.
    hi_geom = get_geom(states_gdf, 'Hawaii')
    hi_polys_proj = list(gpd.GeoSeries(list(hi_geom.geoms), crs=states_gdf.crs)
                         .to_crs('EPSG:6633'))
    hi_proj_xmin = min(p.bounds[0] for p in hi_polys_proj)
    hi_proj_xmax = max(p.bounds[2] for p in hi_polys_proj)
    hi_proj_ymin = min(p.bounds[1] for p in hi_polys_proj)
    hi_proj_ymax = max(p.bounds[3] for p in hi_polys_proj)

    # ── Figure ──────────────────────────────────────────────────────────────
    # Aspect tuned closer to data (CONUS Albers ≈ 1.55) so CONUS actually
    # fills the visible map area instead of leaving cream padding on the
    # sides. 22×16 → aspect 1.375; with map_ax taking ~88% vertical, the
    # ax aspect lands very close to the CONUS data aspect.
    fig = plt.figure(figsize=(22, 16), dpi=100, facecolor=CREAM)

    # Title at top.
    TEXT_X = 0.045
    fig.text(TEXT_X, 0.962, 'How The New York Times Looks At Every State',
             fontsize=24, weight='bold', ha='left', family='serif', color=INK)

    # Single-sentence subtitle, fs sized so it runs two roughly-balanced
    # lines that BOTH stay within the figure's left half (x ≤ 0.5) so the
    # Instagram carousel can split cleanly.
    SUB_FS = 16
    SUB_COLOR = '#4a4438'
    subtitle_lines = [
        'Keywords that The New York Times assigns to its articles show which recurring',
        'subjects are covered in each state out of proportion to national coverage as a whole.',
    ]
    y = 0.926
    for line in subtitle_lines:
        fig.text(TEXT_X, y, line, fontsize=SUB_FS,
                 ha='left', family='serif', color=SUB_COLOR)
        y -= 0.024

    # Main map ax: pushed up tight under the subtitle. Bottom margin
    # leaves room for the methodology block at lower-right.
    map_ax = fig.add_axes([0.005, 0.03, 0.99, 0.86])
    map_ax.set_facecolor(CREAM)
    map_ax.set_aspect('equal')
    map_ax.axis('off')

    # Window: both AK and HI are now in separate insets, so the main
    # map's data range is just CONUS itself. CONUS fills the figure
    # horizontally and vertically.
    pad = conus_w * 0.010
    x0 = conus_bounds[0] - pad
    x1 = conus_bounds[2] + pad * 4   # right margin for callouts
    y0 = conus_bounds[1] - pad
    y1 = conus_bounds[3] + pad
    map_ax.set_xlim(x0, x1)
    map_ax.set_ylim(y0, y1)

    # ── Draw all polygons ───────────────────────────────────────────────────
    def draw_geom(g):
        polys = list(g.geoms) if g.geom_type == 'MultiPolygon' else [g]
        for p in polys:
            if p.is_empty:
                continue
            xs, ys = p.exterior.xy
            map_ax.fill(xs, ys, facecolor=STATE_FILL,
                        edgecolor=STATE_EDGE, linewidth=0.7, zorder=1)

    for _, row in conus.iterrows():
        draw_geom(row.geometry)
    # AK and HI are drawn in their own inset axes below.

    # ── Place labels per state via fit-to-polygon ───────────────────────────
    callouts = []  # (state_name, theme_label, anchor_point_in_data_coords)

    # For the map's single-theme-per-state pick, require the chosen theme
    # to have at least MAP_MIN_ARTICLES and at least MAP_MIN_PCT% of state
    # coverage. Also skip topics that are heavily concentrated in a 1-2
    # year span — those are event-bound disguised as generic tags
    # (e.g. "Serial Murders" in MD/VA = DC Sniper 2002-3, "Fourteenth
    # Amendment" in CO = Trump ballot case 2023-4).
    MAP_MIN_ARTICLES = 5
    MAP_MIN_PCT = 0.5  # %
    YEAR_BURST_THRESHOLD = 0.85  # 2 consecutive years' share

    # Build per-(state, tag) year distribution for the burst check.
    from collections import Counter as _C
    state_tag_years = {}
    for art in arts:
        yr = (art.get('d') or '')[:4]
        if not yr:
            continue
        for st in (art.get('st') or []):
            for tag in (art.get('sb') or []):
                key = (st, tag)
                if key not in state_tag_years:
                    state_tag_years[key] = _C()
                state_tag_years[key][yr] += 1

    def is_year_burst(state_key, tag):
        years = state_tag_years.get((state_key, tag))
        if not years:
            return False
        sorted_counts = sorted(years.values(), reverse=True)
        total = sum(sorted_counts)
        if total < 5:
            return False
        # Check 2-consecutive-year share (any pair of adjacent years)
        sorted_yrs = sorted(years.keys())
        max_pair = 0
        for i in range(len(sorted_yrs) - 1):
            if int(sorted_yrs[i + 1]) == int(sorted_yrs[i]) + 1:
                pair = years[sorted_yrs[i]] + years[sorted_yrs[i + 1]]
                if pair > max_pair:
                    max_pair = pair
        return max_pair / total > YEAR_BURST_THRESHOLD

    def label_for(state_key):
        if state_key not in res:
            return None
        rec = res[state_key]['recurring']
        if not rec:
            return None
        for t in rec:
            if t['count'] >= MAP_MIN_ARTICLES and t['pct'] >= MAP_MIN_PCT:
                if is_year_burst(state_key, t['tag']):
                    continue   # event-bound, skip
                return display_name(t['tag'])
        return display_name(rec[0]['tag'])

    # Process states in order of polygon area, biggest first — bigger states
    # get to claim their visual real estate, smaller states fit around them.
    sorted_rows = sorted(conus.iterrows(),
                         key=lambda kv: -kv[1].geometry.area)
    placed_rects = []
    succeeded, fallbacks = [], []
    for _, row in sorted_rows:
        state_name = row['NAME']
        state_key = 'D.C.' if state_name == 'District of Columbia' else state_name
        text = label_for(state_key)
        if text is None:
            continue
        if state_key in HARD_CALLOUT:
            callouts.append((state_key, text,
                             row.geometry.representative_point()))
            fallbacks.append((state_key, text, 'hard'))
            continue
        result = fit_label(map_ax, fig, row.geometry, text,
                           fs_max=44, fs_min=5, shrink=0.92,
                           max_lines=6, fit_threshold=0.99,
                           placed_rects=placed_rects,
                           override=STATE_OVERRIDES.get(state_key))
        if result is None:
            callouts.append((state_key, text,
                             row.geometry.representative_point()))
            fallbacks.append((state_key, text, 'no_fit'))
        else:
            succeeded.append((state_key, text, result[1], result[2], result[3]))
            placed_rects.append(result[4])
    print(f'Inside-state labels placed: {len(succeeded)}')
    print(f'Callout fallbacks: {len(fallbacks)}')
    for s, t, why in fallbacks:
        print(f'  [{why}] {s}: {t!r}')

    # Alaska — inset placed midway between original "above ME" position
    # and the "above OH" position (about above PA).
    ak_text = label_for('Alaska')
    if ak_text:
        ak_ax = fig.add_axes([0.65, 0.74, 0.15, 0.22])
        ak_ax.set_facecolor(CREAM)
        ak_ax.set_aspect('equal')
        ak_ax.axis('off')
        for p in ak_proj_polys:
            xs, ys = p.exterior.xy
            ak_ax.fill(xs, ys, facecolor=STATE_FILL,
                       edgecolor=STATE_EDGE, linewidth=0.7, zorder=1)
        ak_ax.set_xlim(ak_proj_xmin, ak_proj_xmax)
        ak_ax.set_ylim(ak_proj_ymin, ak_proj_ymax)
        biggest_ak_native = max(ak_proj_polys, key=lambda p: p.area)
        result = fit_label(ak_ax, fig, biggest_ak_native, ak_text,
                           fs_max=24, fs_min=8, shrink=0.92,
                           max_lines=3, fit_threshold=0.94,
                           override=STATE_OVERRIDES.get('Alaska'))
        if result is None:
            force_label(ak_ax, biggest_ak_native, ak_text, fs=12)

    # Hawaii — bottom-left inset. The ax is made slightly taller than the
    # islands actually need so we can place the label in a small strip
    # IMMEDIATELY ABOVE the islands (still inside the ax, parallel to the
    # archipelago, very close to the topmost island).
    hi_text = label_for('Hawaii')
    hi_ax = fig.add_axes([TEXT_X, 0.05, 0.14, 0.16])
    hi_ax.set_facecolor(CREAM)
    hi_ax.set_aspect('equal')
    hi_ax.axis('off')
    for p in hi_polys_proj:
        xs, ys = p.exterior.xy
        hi_ax.fill(xs, ys, facecolor=STATE_FILL,
                   edgecolor=STATE_EDGE, linewidth=0.7, zorder=1)
    hi_w_data = hi_proj_xmax - hi_proj_xmin
    hi_h_data = hi_proj_ymax - hi_proj_ymin
    hi_label_strip = hi_h_data * 0.30   # smaller strip — label sits closer
    hi_ax.set_xlim(hi_proj_xmin, hi_proj_xmax)
    hi_ax.set_ylim(hi_proj_ymin, hi_proj_ymax + hi_label_strip)
    if hi_text:
        cx = hi_proj_xmin + hi_w_data * 0.05    # NW end of chain
        cy = hi_proj_ymax + hi_label_strip * 0.25   # close to islands
        hi_ax.text(cx, cy, hi_text,
                   fontsize=16, rotation=-20,
                   ha='left', va='center',
                   family='serif', weight='semibold', color=INK,
                   zorder=5)

    # ── Callouts: place each label NEAR its state with a short leader ────
    # Specific local positions for the four tiny East-Coast states. A
    # single right-side column would be too far from their actual
    # geography; instead each gets a hand-tuned offset that puts the
    # label just outside the state with a tight leader line.
    CALLOUT_OFFSETS = {
        # state_name : (dx_in_conus_w_units, dy_in_conus_h_units)
        'D.C.':         ( 0.04, -0.035),
        'Maryland':     ( 0.07, -0.005),   # nestled between DE and DC
        'Delaware':     ( 0.06,  0.02),
        'Rhode Island': ( 0.04,  0.05),
    }
    for state_key, text, anchor in callouts:
        dx, dy = CALLOUT_OFFSETS.get(state_key, (0.05, 0.0))
        lx = anchor.x + dx * conus_w
        ly = anchor.y + dy * conus_h
        # Short leader line from polygon's representative point to the label
        map_ax.plot([anchor.x, lx - conus_w * 0.003],
                    [anchor.y, ly],
                    color=LEADER, linewidth=0.7, alpha=0.85, zorder=1.5)
        # State name (small, gray) above the theme name
        map_ax.text(lx, ly + conus_h * 0.011, state_key,
                    fontsize=10, ha='left', va='center',
                    family='serif', color=MUTED)
        map_ax.text(lx, ly - conus_h * 0.005, text,
                    fontsize=13, ha='left', va='center',
                    family='serif', weight='semibold', color=INK)

    # ── Methodology paragraph: column off SE coast. Left-aligned. The
    # token "**most**" in a line is rendered bold inline via HPacker.
    from matplotlib.offsetbox import HPacker, TextArea, AnnotationBbox
    METH_FS = 12
    METH_X = 0.84
    METH_COLOR = '#4a4438'   # darker than MUTED for legibility
    methodology_lines = [
        'This analysis draws on every article',
        'in the U.S. and New York sections',
        'from 2000 to 2026, and depicts the',
        'keyword that was (a) attached to',
        'at least five articles in the state,',
        '(b) was attached to at least 0.5% of',
        'coverage in the state, and (c) was',
        '**most** out of proportion with the',
        'frequency that keyword is employed',
        'nationwide. Keywords exclusive to a',
        'single state, for generic local',
        'electoral and government topics, or',
        'for one-time events such as named',
        'storms and mass shootings are',
        'excluded.',
    ]

    def _render_line(x, y, line):
        if '**most**' not in line:
            fig.text(x, y, line, fontsize=METH_FS,
                     ha='left', family='serif', color=METH_COLOR, zorder=10)
            return
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
        ab = AnnotationBbox(packer, (x, y), xycoords='figure fraction',
                            box_alignment=(0, 0.5), frameon=False, pad=0)
        fig.add_artist(ab)

    y = 0.36
    for line in methodology_lines:
        _render_line(METH_X, y, line)
        y -= 0.020

    # ── Data + URL line: bottom-right corner, single line with bullet ─────
    fig.text(0.99, 0.022,
             'Data from NYT Archive API  •  Full analysis at tedalcorn.github.io/nyt',
             fontsize=12, ha='right', family='serif', color=MUTED, zorder=10)

    out_path = os.path.join(PROJECT_DIR, 'outputs', 'top-keyword',
                            '2026-05-12-us-state-tweets', '-National',
                            'state-map.png')
    plt.savefig(out_path, dpi=400, facecolor=CREAM)   # 4× resolution
    out_svg = out_path.replace('.png', '.svg')
    plt.savefig(out_svg, facecolor=CREAM)
    out_pdf = out_path.replace('.png', '.pdf')
    plt.savefig(out_pdf, facecolor=CREAM)
    plt.close()
    print(f'Saved {out_path}')
    print(f'Saved {out_svg}')
    print(f'Saved {out_pdf}')


if __name__ == '__main__':
    main()
