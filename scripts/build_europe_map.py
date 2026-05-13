"""Europe map: each European country with NYT coverage above the
min-articles threshold gets its #1 recurring theme labeled inside the
country's polygon. Countries below threshold are filled neutral with
no label. Output mirrors the 50-state national map aesthetic.

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
                                 _condense_olympics, OLYMPIC_TAGS)

# Aesthetic — matches state map
CREAM = '#f4efe6'
INK = '#2a2a2a'
MUTED = '#7a7368'
COUNTRY_FILL = '#e8e1d2'
NO_DATA_FILL = '#dfd6c4'
BORDER = '#5a5447'
TITLE_BLUE = '#326891'

# European countries to consider for labeling. Lists geojson-canonical names
# (Natural Earth `NAME` field). Russia is included because its European
# population center is significant, but we'll clip to the visible Europe
# bbox when rendering — only the European bulk shows.
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

# Reverse lookup for geojson name → NYT analysis name
GEOJSON_TO_COUNTRY = {v: k for k, v in COUNTRY_TO_GEOJSON.items()}

# Europe map bbox in LAEA Europe (EPSG:3035) meters, projected from the
# rough lat/lon bbox (-25 W to 45 E, 34 N to 72 N).
EUROPE_BBOX_LATLON = (-25, 34, 45, 72)  # minx, miny, maxx, maxy


def get_country_polys(world_gdf, target_crs):
    """Return dict of {country_display_name: list of projected polygons}
    for European countries only."""
    name_field = 'NAME' if 'NAME' in world_gdf.columns else 'name'
    out = {}
    for _, row in world_gdf.iterrows():
        name = row[name_field]
        if name not in EUROPEAN_COUNTRIES:
            continue
        geom = row.geometry
        # Russia spans the antimeridian — keep only the European bulk
        # (longitudes < 70) so the map doesn't get distorted
        raw = list(geom.geoms) if geom.geom_type == 'MultiPolygon' else [geom]
        if name == 'Russia':
            raw = [p for p in raw if p.bounds[0] < 70 and p.bounds[0] >= 0]
        # Filter far-flung overseas (kept polys within ~30° of largest)
        if len(raw) > 1:
            biggest = max(raw, key=lambda p: p.area)
            bx0, by0, bx1, by1 = biggest.bounds
            bcx = (bx0 + bx1) / 2; bcy = (by0 + by1) / 2
            raw = [p for p in raw
                   if abs((p.bounds[0]+p.bounds[2])/2 - bcx) <= 25
                   and abs((p.bounds[1]+p.bounds[3])/2 - bcy) <= 25]
        if not raw:
            continue
        proj = gpd.GeoSeries(raw, crs=world_gdf.crs).to_crs(target_crs)
        out[name] = list(proj)
    return out


def measure_text_size(ax, fig, text, fs):
    """Estimate text size in data coordinates. Returns (w, h)."""
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


def wrap_options(label, max_lines=4):
    """Yield (n_lines, wrapped_text) options. Splits words evenly."""
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


def fit_label(ax, fig, poly, label, fs_max=22, fs_min=6, max_lines=4):
    """Try font sizes from fs_max down to fs_min, with various wraps.
    Return (anchor_x, anchor_y, fs, wrapped_text) of the best fit,
    or None if nothing fits well."""
    # Anchor at the polygon's representative interior point
    rp = poly.representative_point()
    cx, cy = rp.x, rp.y

    for fs in range(fs_max, fs_min - 1, -1):
        for n_lines, wrapped in wrap_options(label, max_lines=max_lines):
            w, h = measure_text_size(ax, fig, wrapped, fs)
            # Test box centered on rp
            test_box = ShpPolygon([
                (cx - w/2, cy - h/2), (cx + w/2, cy - h/2),
                (cx + w/2, cy + h/2), (cx - w/2, cy + h/2),
            ])
            if poly.contains(test_box):
                return cx, cy, fs, wrapped
    return None


def main():
    print('Loading country geometries…')
    geo_path = os.path.join(PROJECT_DIR, 'data', 'world_countries.geojson')
    world_gdf = gpd.read_file(geo_path)
    target_crs = 'EPSG:3035'  # LAEA Europe

    print('Running theme analysis…')
    arts = load_world_articles()
    res = analyze(arts)

    # Map geojson names → analysis names so we can look up themes
    # Some European country names are identical (Greece, Germany);
    # others differ (United Kingdom in geojson = Britain in analysis).
    european_polys = get_country_polys(world_gdf, target_crs)
    print(f'  {len(european_polys)} European countries with geometries')

    # Compute bbox in LAEA Europe — project the lat/lon corners
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
    bbox_aspect = (bbox_maxx - bbox_minx) / (bbox_maxy - bbox_miny)

    # ── Figure ─────────────────────────────────────────────────────────
    # Figure aspect matches Europe's natural aspect (1.66:1 in LAEA), plus
    # extra vertical room for title + footer. Map area is 14 wide.
    map_w = 14
    map_h = map_w / bbox_aspect
    fig_h = map_h + 3.0   # 1.8 top for title, 1.2 bottom for footer
    fig = plt.figure(figsize=(map_w, fig_h), dpi=120, facecolor=CREAM)
    # Map axes: full width, vertically positioned so title sits above
    ax = fig.add_axes([0.02, 0.05 + 1.2/fig_h, 0.96, map_h/fig_h])
    ax.set_facecolor(CREAM)
    ax.set_aspect('equal')
    ax.axis('off')

    # Title — anchored to the top of the figure
    title_y_main = 1.0 - 0.35 / fig_h
    title_y_country = 1.0 - 0.95 / fig_h
    title_y_sub = 1.0 - 1.40 / fig_h
    fig.text(0.5, title_y_main, "How The New York Times Looks at",
             fontsize=24, family='serif', weight='semibold',
             color=INK, ha='center')
    fig.text(0.5, title_y_country, "EUROPE",
             fontsize=44, family='serif', weight='bold',
             color=TITLE_BLUE, ha='center')
    fig.text(0.5, title_y_sub,
             "The subject that each country's NYT World coverage features "
             "most out of proportion to international coverage as a whole.",
             fontsize=13, family='serif', color='#4a4438', ha='center')

    ax.set_xlim(bbox_minx, bbox_maxx)
    ax.set_ylim(bbox_miny, bbox_maxy)

    # Draw all countries first (gray for no-data, fill for has-data)
    for gname, polys in european_polys.items():
        analysis_name = GEOJSON_TO_COUNTRY.get(gname, gname)
        has_data = analysis_name in res
        fill_color = COUNTRY_FILL if has_data else NO_DATA_FILL

        # Drop shadow per polygon (very subtle, paper-on-paper)
        for poly in polys:
            if not poly.exterior:
                continue
            xs2, ys2 = poly.exterior.xy
            rng_x = max(xs2) - min(xs2)
            rng_y = max(ys2) - min(ys2)
            ox = rng_x * 0.005
            oy = rng_y * 0.005
            ax.fill([x + ox for x in xs2], [y - oy for y in ys2],
                    facecolor='#c2b9a3', edgecolor='none',
                    alpha=0.35, zorder=1)

        # Fill
        for poly in polys:
            if not poly.exterior:
                continue
            xs2, ys2 = poly.exterior.xy
            ax.fill(xs2, ys2, facecolor=fill_color, edgecolor='none',
                    zorder=1.5)

        # Outline
        for poly in polys:
            if not poly.exterior:
                continue
            xs2, ys2 = poly.exterior.xy
            ax.plot(xs2, ys2, color=BORDER, linewidth=0.8,
                    solid_joinstyle='round', zorder=3)

    # Label each country with its top recurring theme
    callouts = []  # countries too small for in-polygon labels
    for gname, polys in european_polys.items():
        analysis_name = GEOJSON_TO_COUNTRY.get(gname, gname)
        if analysis_name not in res:
            continue
        country_res = res[analysis_name]
        recurring = country_res.get('recurring', [])
        tag_years = country_res.get('tag_years', {})
        if not recurring:
            continue
        # Apply Olympic condensation for display consistency with cards
        recurring = _condense_olympics(recurring, tag_years)
        top = recurring[0]
        label = display_name(top['tag'], analysis_name)

        # Combine all polys into one shape for label placement (use largest).
        # Clip to the visible bbox so countries that extend beyond Europe
        # (Russia, Turkey) get labels placed within the European portion only.
        bbox_rect = ShpPolygon([
            (bbox_minx, bbox_miny), (bbox_maxx, bbox_miny),
            (bbox_maxx, bbox_maxy), (bbox_minx, bbox_maxy),
        ])
        clipped = [p.intersection(bbox_rect) for p in polys]
        clipped = [p for p in clipped if not p.is_empty and p.area > 0]
        if not clipped:
            continue
        # Take the largest individual polygon piece after clipping
        all_pieces = []
        for cp in clipped:
            if cp.geom_type == 'Polygon':
                all_pieces.append(cp)
            elif cp.geom_type == 'MultiPolygon':
                all_pieces.extend(cp.geoms)
        biggest = max(all_pieces, key=lambda p: p.area)
        fit = fit_label(ax, fig, biggest, label,
                        fs_max=20, fs_min=6, max_lines=3)
        if fit is None:
            # Too small for in-polygon label — callout instead
            rp = biggest.representative_point()
            callouts.append((analysis_name, label, rp.x, rp.y))
            continue
        cx, cy, fs, wrapped = fit
        ax.text(cx, cy, wrapped,
                ha='center', va='center',
                fontsize=fs, family='serif', weight='semibold',
                color=INK, zorder=4)

    # Render callouts off to the side (right edge of map)
    if callouts:
        xmin, xmax = ax.get_xlim()
        ymin, ymax = ax.get_ylim()
        # Place callouts in a column down the right side
        co_x = xmax - (xmax - xmin) * 0.08
        n = len(callouts)
        for i, (cname, label, px, py) in enumerate(sorted(callouts)):
            co_y = ymax - (ymax - ymin) * (0.05 + i * 0.04)
            ax.annotate(f"{cname}: {label}",
                        xy=(px, py), xytext=(co_x, co_y),
                        fontsize=8, family='serif', color=MUTED,
                        ha='right', va='center',
                        arrowprops=dict(arrowstyle='-', color=MUTED,
                                        linewidth=0.5),
                        zorder=4)

    # Methods footer
    fig.text(0.5, 0.025,
             "Methods: Analysis of NYT-assigned keywords in World coverage, "
             "2000–2026. Subjects are scored by overrepresentation versus "
             "World coverage overall.  ·  Data from NYT Archive API  ·  "
             "tedalcorn.github.io/nyt",
             fontsize=10, ha='center', family='serif', color=MUTED)

    out_dir = os.path.join(PROJECT_DIR, 'outputs', 'top-keyword', 'World map')
    os.makedirs(out_dir, exist_ok=True)
    out_png = os.path.join(out_dir, 'europe-map.png')
    plt.savefig(out_png, dpi=120, facecolor=CREAM)
    plt.close()
    print(f'  Saved {out_png}')

    if callouts:
        print(f'\nCallouts ({len(callouts)} countries too small for in-polygon labels):')
        for cname, label, _, _ in sorted(callouts):
            print(f'  {cname}: {label}')


if __name__ == '__main__':
    main()
