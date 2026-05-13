"""Americas map: each country in the Americas (excluding the US, which is
covered as a 50-state series) gets its #1 recurring theme labeled inside
the country's polygon.

Same aesthetic as the Europe map: 16:9 country outline + per-country fit
labels, methodology paragraph integrated, high-DPI PNG + PDF.

Run:
    python3 scripts/build_americas_map.py

Output: outputs/2026-05-top-keyword/2026-05-13-world-country-tweets/Americas/
        americas-map.{png,pdf}
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
# US deliberately excluded — already covered by the 50-state series.
AMERICAS_COUNTRIES = {
    'Canada', 'United States of America', 'Mexico',
    'Belize', 'Guatemala', 'El Salvador', 'Honduras', 'Nicaragua',
    'Costa Rica', 'Panama',
    'Cuba', 'Haiti', 'Dominican Rep.', 'Jamaica', 'Bahamas',
    'Trinidad and Tobago', 'Barbados',
    'Argentina', 'Brazil', 'Bolivia', 'Chile', 'Colombia', 'Ecuador',
    'Guyana', 'Paraguay', 'Peru', 'Suriname', 'Uruguay', 'Venezuela',
    'Falkland Is.',  # for completeness; usually unlabeled
}

# Countries to render with a distinguishing crosshatch fill but no label —
# they belong on the map for geographic context but their themes are
# covered elsewhere in the series (e.g. the US has its own 50-state map).
HATCHED_COUNTRIES = {'United States of America'}

# geojson NAME → analysis country name (only when they differ)
GEOJSON_TO_ANALYSIS = {
    'Dominican Rep.': 'Dominican Republic',
    'Falkland Is.': 'Falkland Islands',
}

# Short display names for the "Name:" caption — keeps tiny-country labels
# readable.
COUNTRY_DISPLAY_NAME = {
    'Dominican Rep.': 'Dominican Rep.',
    'Trinidad and Tobago': 'Trinidad',
    'El Salvador': 'El Salvador',
    'Costa Rica': 'Costa Rica',
}

# Per-country fit overrides. Same shape as EUROPE_OVERRIDES.
AMERICAS_OVERRIDES = {
    # Big countries get much larger fonts to fill their area
    'Canada':    {'forced_text': 'Oil\nSands', 'fs_max': 90,
                  'anchor_y_frac': 0.30},
    'Mexico':    {'forced_text': 'Drug\nCartels', 'fs_max': 38,
                  'rotations': [-30, 0]},  # tilt to match the country's NW-SE axis
    'Brazil':    {'forced_text': 'Carnival', 'fs_max': 80},
    'Argentina': {'forced_text': 'Defaulting', 'fs_max': 16,
                  'rotations': [70, 0]},
    'Chile':     {'forced_text': 'Wildfires', 'fs_max': 10,
                  'rotations': [80, 0]},
    'Peru':      {'forced_text': 'Incas', 'fs_max': 18},
    'Colombia':  {'fs_max': 14},  # let auto-pick the top non-event tag
    'Venezuela': {'fs_max': 13},
    'Ecuador':   {'fs_max': 10},
    'Bolivia':   {'fs_max': 13},
    'Paraguay':  {'fs_max': 10},
    'Uruguay':   {'fs_max': 8},
    'Guyana':    {'fs_max': 8},
    'Suriname':  {'fs_max': 8},
    # Cuba is wide but very thin (E-W ~10× the N-S extent in projection).
    # Single-line label with lower fit_threshold so it can extend slightly
    # past the polygon into the ocean above/below.
    'Cuba':      {'forced_text': 'Cuban-Americans', 'fs_max': 11,
                  'rotations': [-15], 'fit_threshold': 0.65},
    # Haiti/DR/Jamaica too small for in-polygon; use callouts (see below)
    # Bahamas: rotation matches the archipelago's NW-SE chain
    'Bahamas':   {'forced_text': 'Hurricanes', 'fs_max': 10,
                  'rotations': [-45]},
    'Guatemala': {'forced_text': 'Mayans', 'fs_max': 12},
    'Honduras':  {'forced_text': 'Gangs', 'fs_max': 11},
    'Nicaragua': {'fs_max': 10},
    'Costa Rica': {'fs_max': 10},
    'Panama':    {'forced_text': 'Canals', 'fs_max': 10,
                  'rotations': [-30]},
    'El Salvador': {'fs_max': 9},
    'Belize':    {'fs_max': 9},
    'Trinidad and Tobago': {'fs_max': 8},
    'Barbados':  {'fs_max': 8},
}

# Tiny countries that should always go to callout (no in-polygon labeling).
# Offsets are (dx, dy) as fractions of the visible map width/height. The
# Caribbean cluster fans out in four directions so callouts don't stack:
#   - Northern islands (Cuba/Bahamas) get their leaders pulled NORTH
#   - Southern islands (Jamaica/Haiti/DR) labeled from BELOW (Caribbean Sea)
#     so they don't pile up over Cuba's area
CALLOUT_OFFSETS = {
    'Haiti':               (-0.02, -0.07),   # south into Caribbean Sea
    'Dominican Rep.':      ( 0.04, -0.07),   # south-east into Caribbean
    'Jamaica':             (-0.08, -0.04),   # SW into Caribbean
    'Trinidad and Tobago': ( 0.05, -0.005),
    'Barbados':            ( 0.05,  0.005),
    'Falkland Is.':        (-0.03, -0.02),
}

# Bbox in lat/lon — covers Tierra del Fuego (lat -56) up to the Canadian
# Arctic (lat 75), and from the Pacific west of Alaska (lon -140) east to
# the Brazilian coast (lon -33). Slightly extended past prior version so
# Canada's full northern extent and Mexico's western coast aren't cropped.
AMERICAS_BBOX_LATLON = (-140, -57, -33, 78)  # minx, miny, maxx, maxy

# Threshold for showing a country's label at all
MIN_SCORE_TO_LABEL = 6.0
MIN_TAG_YEAR_SPAN = 2

# Countries explicitly excluded
SKIP_COUNTRIES = {'United States'}


def pick_americas_projection():
    """Lambert Azimuthal Equal-Area centered on the Americas centroid.
    Preserves area; produces accurate local shapes across the huge
    north-south span. Center ~-65°W, 5°N is roughly the visual center."""
    return '+proj=laea +lat_0=5 +lon_0=-65 +datum=WGS84 +units=m +no_defs'


def get_country_polys(world_gdf, target_crs):
    name_field = 'NAME' if 'NAME' in world_gdf.columns else 'name'
    out = {}
    for _, row in world_gdf.iterrows():
        name = row[name_field]
        if name not in AMERICAS_COUNTRIES:
            continue
        geom = row.geometry
        raw = list(geom.geoms) if geom.geom_type == 'MultiPolygon' else [geom]
        # Drop dateline-crossing fragments (Alaska-style problem for Russia
        # doesn't apply here, but keep the safety check generic)
        xmin_all, _, xmax_all, _ = geom.bounds
        if (xmax_all - xmin_all) > 180:
            east = sum(p.area for p in raw if p.bounds[0] >= 0)
            west = sum(p.area for p in raw if p.bounds[2] < 0)
            keep_negative = west >= east
            raw = [p for p in raw if (p.bounds[2] < 0) == keep_negative]
        # Drop far-flung outliers (overseas territories, distant islands)
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


def candidate_anchors(poly, anchor_y_frac=None):
    pts = [poly.representative_point(), poly.centroid]
    minx, miny, maxx, maxy = poly.bounds
    if anchor_y_frac is not None:
        cy = miny + (maxy - miny) * anchor_y_frac
        cx = (minx + maxx) / 2
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
    fit_threshold = override.get('fit_threshold', 0.97)
    forced_text = override.get('forced_text')

    anchors = list(candidate_anchors(poly, anchor_y_frac=anchor_y_frac))
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
    target_crs = pick_americas_projection()

    print('Running theme analysis…')
    arts = load_world_articles()
    res = analyze(arts)
    print(f'  {len(res)} countries scored')

    americas_polys = get_country_polys(world_gdf, target_crs)
    print(f'  {len(americas_polys)} country geometries')

    # Compute bbox in projection meters
    bbox_pts = gpd.GeoSeries([
        Point(AMERICAS_BBOX_LATLON[0], AMERICAS_BBOX_LATLON[1]),
        Point(AMERICAS_BBOX_LATLON[2], AMERICAS_BBOX_LATLON[1]),
        Point(AMERICAS_BBOX_LATLON[0], AMERICAS_BBOX_LATLON[3]),
        Point(AMERICAS_BBOX_LATLON[2], AMERICAS_BBOX_LATLON[3]),
    ], crs=world_gdf.crs).to_crs(target_crs)
    bxs = [p.x for p in bbox_pts]
    bys = [p.y for p in bbox_pts]
    bbox_minx, bbox_maxx = min(bxs), max(bxs)
    bbox_miny, bbox_maxy = min(bys), max(bys)
    eur_w = bbox_maxx - bbox_minx
    eur_h = bbox_maxy - bbox_miny
    bbox_aspect = eur_w / eur_h

    # Portrait orientation — the Americas is taller than wide
    map_h_inches = 18
    map_w_inches = map_h_inches * bbox_aspect
    fig_w = map_w_inches
    fig_h = map_h_inches + 2.0
    fig = plt.figure(figsize=(fig_w, fig_h), dpi=120, facecolor=CREAM)
    map_ax = fig.add_axes([0.0, 0.6 / fig_h, 1.0, map_h_inches / fig_h])
    map_ax.set_facecolor(CREAM)
    map_ax.set_aspect('equal')
    map_ax.axis('off')
    map_ax.set_xlim(bbox_minx, bbox_maxx)
    map_ax.set_ylim(bbox_miny, bbox_maxy)

    # Title — wrapped to 2 lines (portrait orientation = narrow figure
    # width). Subtitle also wraps to 2 lines.
    title_y = 1.0 - 0.45 / fig_h
    fig.text(0.02, title_y,
             "How The New York Times",
             fontsize=28, family='serif', weight='semibold',
             color=INK, ha='left', va='top')
    fig.text(0.02, title_y - 0.50 / fig_h,
             "Looks At The Americas",
             fontsize=28, family='serif', weight='semibold',
             color=INK, ha='left', va='top')
    fig.text(0.02, title_y - 1.05 / fig_h,
             "Keywords that The New York Times assigns to its articles show which",
             fontsize=12, family='serif', color='#4a4438',
             ha='left', va='top')
    fig.text(0.02, title_y - 1.30 / fig_h,
             "recurring subjects are covered in each country out of proportion to",
             fontsize=12, family='serif', color='#4a4438',
             ha='left', va='top')
    fig.text(0.02, title_y - 1.55 / fig_h,
             "international coverage as a whole.",
             fontsize=12, family='serif', color='#4a4438',
             ha='left', va='top')

    # Draw all countries
    from matplotlib.path import Path as MplPath
    from matplotlib.patches import PathPatch
    for gname, polys in americas_polys.items():
        analysis_name = GEOJSON_TO_ANALYSIS.get(gname, gname)
        hatched = gname in HATCHED_COUNTRIES
        has_data = analysis_name in res
        fill = COUNTRY_FILL if has_data else NO_DATA_FILL

        # Drop shadow
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
        # Base fill
        for poly in polys:
            if not poly.exterior: continue
            xs, ys = poly.exterior.xy
            if hatched:
                # Lighter base fill so the hatching reads visibly without
                # disappearing the country.
                map_ax.fill(xs, ys, facecolor='#eee6d3', edgecolor='none',
                            zorder=1.5)
            else:
                map_ax.fill(xs, ys, facecolor=fill, edgecolor='none', zorder=1.5)
        # Hatching for US-style "covered elsewhere" countries
        if hatched:
            for poly in polys:
                if not poly.exterior: continue
                xs, ys = poly.exterior.xy
                verts = list(zip(xs, ys))
                codes = [MplPath.MOVETO] + [MplPath.LINETO] * (len(verts) - 2) + [MplPath.CLOSEPOLY]
                patch = PathPatch(MplPath(verts, codes),
                                  facecolor='none',
                                  edgecolor='#a89d83',
                                  hatch='////',
                                  linewidth=0,
                                  zorder=2)
                map_ax.add_patch(patch)
        # Border outline
        for poly in polys:
            if not poly.exterior: continue
            xs, ys = poly.exterior.xy
            map_ax.plot(xs, ys, color=BORDER, linewidth=0.7,
                        solid_joinstyle='round', zorder=3)

    # Labels — skip hatched countries (US) and explicit skip list
    callouts = []
    for gname, polys in americas_polys.items():
        analysis_name = GEOJSON_TO_ANALYSIS.get(gname, gname)
        if analysis_name in SKIP_COUNTRIES or gname in HATCHED_COUNTRIES:
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

        override = AMERICAS_OVERRIDES.get(gname, {})
        fit = fit_label(map_ax, fig, biggest, label, override)
        if fit is None:
            callouts.append((gname, label, biggest.representative_point()))
            continue
        cx, cy, fs, rotation, text = fit

        # Country caption above
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

    # Callouts
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

    # Methodology
    n_world_articles = sum(1 for a in arts if (a.get('s') or '') == 'World')
    rounded_articles = f"{round(n_world_articles, -3):,.0f}"
    from matplotlib.offsetbox import HPacker, TextArea, AnnotationBbox
    METH_COLOR = '#4a4438'
    # Methodology — narrower column so lines stay west of Chile/Argentina.
    # Each line ~50 chars; block sits in the south Pacific (lower-left)
    # at figure y ≈ 0.10-0.35. That latitude band is open water on the
    # Americas map (Chile's south coast is at lat ~-56°, well below the
    # methodology block; Pacific west of South America is fully empty).
    methodology_lines = [
        f'This map draws on {rounded_articles} articles in the World',
        'section from 2000 to 2026. The New York Times assigns each',
        'article subject keywords (separate from tags for individual',
        'people and organizations, which are not included here). For',
        'each country with sufficient coverage to identify recurring',
        'patterns, the map shows the keyword that (a) appeared on at',
        'least 1% of the country’s coverage and (b) was **most** out',
        'of proportion with that keyword’s frequency in World coverage',
        'overall. The analysis excludes each country’s own name and',
        'currency, broad topics applied to most countries such as',
        '“international relations,” and one-time events such as named',
        'storms, major accidents, and specific Olympic Games.',
    ]
    METH_X = 0.025
    METH_FS = 9
    LINE_SPACING = 0.0145
    # Lower-left placement: block top at y ≈ 0.30, bottom around 0.12.
    # That's the south Pacific west of Chile/Patagonia — verified empty
    # on the Americas LAEA projection.
    y = 0.295
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

    fig.text(0.98, 0.02,
             'Data from NYT Archive API  •  Full analysis at tedalcorn.github.io/nyt',
             fontsize=11, ha='right', family='serif', color=MUTED, zorder=10)

    out_dir = os.path.join(PROJECT_DIR, 'outputs', '2026-05-top-keyword',
                           '2026-05-13-world-country-tweets', 'Americas')
    os.makedirs(out_dir, exist_ok=True)
    out_png = os.path.join(out_dir, 'americas-map.png')
    out_pdf = os.path.join(out_dir, 'americas-map.pdf')
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
