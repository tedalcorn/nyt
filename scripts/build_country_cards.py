"""Per-country Twitter-thread cards — parallel to build_state_cards.py.

Each card: country outline (with capital star) + top-3 recurring themes
with overrepresentation factor and share of country coverage.

Aesthetic matches the state cards (cream background, dark slate ink, serif
typography, single accent blue, drop shadow, 16:9 format).

Run:
    python3 scripts/build_country_cards.py [COUNTRY [COUNTRY ...]]
    python3 scripts/build_country_cards.py Greece Germany Ukraine  # prototype
    python3 scripts/build_country_cards.py                          # all top-50

Output: outputs/2026-05-top-keyword/2026-05-13-world-country-tweets/countries/<Region>/<country-slug>.png
"""
import os
import re
import sys
import matplotlib.pyplot as plt
from matplotlib.path import Path as MplPath
from matplotlib.patches import PathPatch
import geopandas as gpd
from shapely.geometry import Point

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_DIR, 'scripts'))
from build_country_keywords import load_world_articles, analyze

# Visual palette — identical to the state cards
CREAM = '#f4efe6'
INK = '#2a2a2a'
MUTED = '#7a7368'
COUNTRY_FILL = '#e8e1d2'
TITLE_BLUE = '#326891'

# Display-name overrides for awkward NYT tag forms (parallel to THEME_DISPLAY
# in build_state_cards.py — kept separate so country-specific shortenings
# don't bleed into state cards).
THEME_DISPLAY = {
    'SARS (Severe Acute Respiratory Syndrome)': 'SARS',
    'Drones (Pilotless Planes)': 'Drones',
    'Tidal Waves and Tsunamis': 'Tsunamis',
    'Uighurs (Chinese Ethnic Group)': 'Uighurs',
    'Han Chinese (Ethnic Group)': 'Han Chinese',
    'Pashtun (Ethnic Group)': 'Pashtun',
    'Yazidi (Religious Sect)': 'Yazidi',
    'Mercenaries and Private Military Contractors': 'Mercenaries',
    'War Crimes, Genocide and Crimes Against Humanity': 'War Crimes',
    'Missiles and Missile Defense Systems': 'Missiles',
    'Refugees and Displaced Persons': 'Refugees',
    'Civil War and Guerrilla Warfare': 'Civil War',
    'Civilian Casualties': 'Civilian Casualties',
    'Power Failures and Blackouts': 'Power Failures',
    'Indecency, Obscenity and Profanity': 'Indecency',
    'Hurricanes and Tropical Storms': 'Hurricanes',
    'Holocaust and the Nazi Era': 'Holocaust',
    'Defectors (Political)': 'Defectors',
    'Asylum (Political)': 'Political Asylum',
    'Korean War': 'Korean War',
    'Whales and Whaling': 'Whaling',
    'Cole (USS)': 'USS Cole Attack',
    'United States Defense and Military Forces': 'US Military',
    'Iran-Israel Proxy Conflict': 'Iran Proxy Conflict',
    'Israeli Settlements': 'Israeli Settlements',
    'Books and Literature': 'Books',
    'Drug Abuse and Traffic': 'Drug Trafficking',
    'Voting and Voters': 'Voting',
    'Territorial Disputes': 'Territorial Disputes',
    'Sex Crimes': 'Sex Crimes',
    'Cricket (Game)': 'Cricket',
    'Air Pollution': 'Air Pollution',
    'Cricket (Game)': 'Cricket',
    'Election Issues': 'Election Issues',
    'War Crimes and Criminals': 'War Crimes (people)',
    'Olympic Games (2004)': 'Athens 2004 Olympics',
    'Olympic Games (2008)': 'Beijing 2008 Olympics',
    'Olympic Games (2012)': 'London 2012 Olympics',
    'Olympic Games (2016)': 'Rio 2016 Olympics',
    'Olympic Games (2018)': 'Pyeongchang 2018 Olympics',
    'Olympic Games (2020)': 'Tokyo 2020 Olympics',
    'Olympic Games (2024)': 'Paris 2024 Olympics',
    'Olympic Games (2026)': 'Milan/Cortina 2026 Olympics',
    'Jews and Judaism': 'Jews',
    'Muslims and Islam': 'Islam',
    'Russian Invasion of Ukraine (2022)': 'Russia-Ukraine War',
    'Israel-Gaza War (2023- )': 'Israel-Gaza War',
    'Iraq War (2003-11)': 'Iraq War',
    'Afghanistan War (2001- )': 'Afghanistan War',
    'Japan Earthquake and Tsunami (2011)': '2011 Tohoku Disaster',
    'World War II (1939-45)': 'World War II',
    'World War I (1914-18)': 'World War I',
    'Church of the Nativity (Bethlehem)': 'Church of the Nativity Siege',
    'Temple Mount (Jerusalem)': 'Temple Mount',
    'Fukushima Daiichi Nuclear Power Plant (Japan)': 'Fukushima Daiichi',
    'Pentagon Building': 'Pentagon',
    'World Trade Center (NYC)': 'World Trade Center',
    'Severe Acute Respiratory Syndrome (Sars)': 'SARS',
    'Restoration and Renovation': 'Restoration',
}

# Capital city coordinates for top-50 country geographic anchors.
# Lon, lat (WGS84). Only used to draw a small red star on the country outline.
COUNTRY_CAPITALS = {
    'Afghanistan': ('Kabul', 69.1833, 34.5333),
    'Argentina': ('Buenos Aires', -58.3816, -34.6037),
    'Australia': ('Canberra', 149.1300, -35.2809),
    'Belgium': ('Brussels', 4.3517, 50.8503),
    'Brazil': ('Brasília', -47.8825, -15.7942),
    'Britain': ('London', -0.1278, 51.5074),
    'Canada': ('Ottawa', -75.6972, 45.4215),
    'China': ('Beijing', 116.4074, 39.9042),
    'Cuba': ('Havana', -82.3666, 23.1136),
    'Democratic Republic of Congo': ('Kinshasa', 15.2663, -4.4419),
    'Egypt': ('Cairo', 31.2357, 30.0444),
    'France': ('Paris', 2.3522, 48.8566),
    'Gaza Strip': ('Gaza City', 34.4668, 31.5018),
    'Georgia': ('Tbilisi', 44.7833, 41.7167),
    'Germany': ('Berlin', 13.4050, 52.5200),
    'Great Britain': ('London', -0.1278, 51.5074),
    'Greece': ('Athens', 23.7275, 37.9838),
    'Hong Kong': ('Hong Kong', 114.1694, 22.3193),
    'India': ('New Delhi', 77.2090, 28.6139),
    'Indonesia': ('Jakarta', 106.8456, -6.2088),
    'Iran': ('Tehran', 51.3890, 35.6892),
    'Iraq': ('Baghdad', 44.3661, 33.3152),
    'Ireland': ('Dublin', -6.2603, 53.3498),
    'Israel': ('Jerusalem', 35.2137, 31.7683),
    'Italy': ('Rome', 12.4964, 41.9028),
    'Japan': ('Tokyo', 139.6917, 35.6895),
    'Kenya': ('Nairobi', 36.8219, -1.2921),
    'Kosovo': ('Pristina', 21.1655, 42.6629),
    'Lebanon': ('Beirut', 35.5018, 33.8938),
    'Libya': ('Tripoli', 13.1913, 32.8872),
    'Mexico': ('Mexico City', -99.1332, 19.4326),
    'Netherlands': ('Amsterdam', 4.9041, 52.3676),
    'Nigeria': ('Abuja', 7.4951, 9.0579),
    'North Korea': ('Pyongyang', 125.7625, 39.0392),
    'Pakistan': ('Islamabad', 73.0479, 33.6844),
    'Philippines': ('Manila', 120.9842, 14.5995),
    'Russia': ('Moscow', 37.6173, 55.7558),
    'Saudi Arabia': ('Riyadh', 46.6753, 24.7136),
    'Serbia': ('Belgrade', 20.4612, 44.7866),
    'South Africa': ('Pretoria', 28.1881, -25.7479),
    'South Korea': ('Seoul', 126.9780, 37.5665),
    'Spain': ('Madrid', -3.7038, 40.4168),
    'Sudan': ('Khartoum', 32.5599, 15.5007),
    'Switzerland': ('Bern', 7.4474, 46.9480),
    'Syria': ('Damascus', 36.2765, 33.5138),
    'Taiwan': ('Taipei', 121.5654, 25.0330),
    'Thailand': ('Bangkok', 100.5018, 13.7563),
    'Turkey': ('Ankara', 32.8597, 39.9334),
    'Ukraine': ('Kyiv', 30.5234, 50.4501),
    'United States': ('Washington', -77.0369, 38.9072),
    'West Bank': ('Ramallah', 35.2010, 31.9038),
}

# NYT country names → Natural Earth geojson names (where they differ)
COUNTRY_TO_GEOJSON = {
    'Britain': 'United Kingdom',
    'Great Britain': 'United Kingdom',
    'Democratic Republic of Congo': 'Dem. Rep. Congo',
    'United States': 'United States of America',
    'South Korea': 'South Korea',
    'North Korea': 'North Korea',
    'Czech Republic': 'Czechia',
}


# Per-(country, tag) display overrides — apply when a generic tag has a
# country-specific meaning that benefits from a parenthetical clarification.
# Verified case-by-case from the underlying article headlines, not auto-
# inferred. Only used for clearly-skewed cases (≥90% of the country's
# articles for that tag share the same context).
COUNTRY_TAG_DISPLAY = {
    ('Indonesia', 'Recording Equipment'): 'Black Boxes (Plane Crashes)',
    ('North Macedonia', 'Names, Geographical'): 'Dispute over country renaming',
    ('Macedonia', 'Names, Geographical'): 'Dispute over country renaming',
}


def display_name(tag, country=None):
    if country is not None:
        override = COUNTRY_TAG_DISPLAY.get((country, tag))
        if override:
            return override
    return THEME_DISPLAY.get(tag, tag)


def slugify(name):
    return re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-')


def fmt_pct(pct):
    if pct < 1:
        return f'{pct:.2f}%'
    if pct < 10:
        return f'{pct:.1f}%'
    return f'{pct:.0f}%'


def pick_projection(geom_centroid_lat, geom_centroid_lon):
    """Per-country Lambert Azimuthal Equal-Area projection centered on the
    country's centroid. Preserves area and produces accurate local shapes
    regardless of country size or latitude — works equally well for small
    Mediterranean countries, sprawling Canada/Russia/China, and equatorial
    Indonesia. Cylindrical projections (Behrmann) flattened high-latitude
    countries badly and Conus Albers only works for a narrow longitude
    band; centering LAEA on each country sidesteps both problems.
    """
    lat = geom_centroid_lat
    lon = geom_centroid_lon
    return f'+proj=laea +lat_0={lat} +lon_0={lon} +datum=WGS84 +units=m +no_defs'


OLYMPIC_TAGS = {'Olympic Games', 'Summer Games (Olympics)', 'Winter Games (Olympics)'}

def _condense_olympics(recurring, tag_years):
    """When a country has generic Olympic tags in its recurring list, replace
    them with a single host-year-specific display label. NYT tags Olympic
    coverage inconsistently — some articles get the year-specific tag
    ('Olympic Games (2004)' — already a headline event) and some only get the
    generic ('Olympic Games' or 'Summer Games (Olympics)'). The recurring list
    has only the generic ones, but they nearly always cluster in the country's
    host year(s), so we display them as 'Athens 2004 Olympics' etc."""
    olys = [i for i, t in enumerate(recurring) if t['tag'] in OLYMPIC_TAGS]
    if not olys:
        return recurring
    # Pick the highest-scoring of the generic Olympic entries
    keep_idx = olys[0]  # recurring is already sorted by score desc
    keep_item = recurring[keep_idx]
    # Modal year across ALL Olympic-tagged articles for this country
    year_counts = {}
    for t in OLYMPIC_TAGS:
        ty = tag_years.get(t, {})
        for y, c in ty.items():
            year_counts[y] = year_counts.get(y, 0) + c
    if year_counts:
        peak_year = max(year_counts.items(), key=lambda kv: kv[1])[0]
        season = ('Winter' if 'Winter' in keep_item['tag'] else 'Summer')
        keep_item = {
            **keep_item,
            'tag': f'{season} Olympics ({peak_year})',
        }
    # Build new list: keep the chosen entry, drop the other generic Olympic ones
    drop = set(olys) - {keep_idx}
    return [keep_item if i == keep_idx else item
            for i, item in enumerate(recurring) if i not in drop]


def make_card(country, recurring, output_path, world_gdf, n_themes=5,
              tag_years=None):
    if tag_years:
        recurring = _condense_olympics(recurring, tag_years)
    name_field = 'NAME' if 'NAME' in world_gdf.columns else 'name'
    geojson_name = COUNTRY_TO_GEOJSON.get(country, country)
    geom_row = world_gdf[world_gdf[name_field] == geojson_name]
    if geom_row.empty:
        print(f'  Skipping {country} — no geometry (looked for {geojson_name!r})')
        return False
    geom = geom_row.iloc[0].geometry

    # Use centroid in lat/lon to pick projection
    centroid = geom.centroid
    target_crs = pick_projection(centroid.y, centroid.x)

    raw_polys = list(geom.geoms) if geom.geom_type == 'MultiPolygon' else [geom]

    # Filter out polygons that mess up rendering:
    #   - Antimeridian-crossing fragments. Russia, Fiji, Kiribati, and
    #     others have small polygons near +180/-180 longitudes. When the
    #     country's overall bbox spans both hemispheres, drop the minority
    #     fragments so the main body renders cleanly.
    #   - Overseas territories far from the main body. France has French
    #     Guiana / Reunion / Mayotte; the Netherlands has Caribbean islands.
    #     Drop sub-polygons whose distance from the largest polygon's
    #     centroid is more than ~30° great-circle in either dimension.
    xmin_all, _, xmax_all, _ = geom.bounds
    spans_antimeridian = (xmax_all - xmin_all) > 180
    if spans_antimeridian:
        # Keep only polygons on the side where most of the area lives.
        east_area = sum(p.area for p in raw_polys if p.bounds[0] >= 0)
        west_area = sum(p.area for p in raw_polys if p.bounds[2] < 0)
        if east_area >= west_area:
            raw_polys = [p for p in raw_polys if p.bounds[0] >= 0]
        else:
            raw_polys = [p for p in raw_polys if p.bounds[2] < 0]

    # Drop far-flung outliers — keep only sub-polygons whose bbox center
    # is within ~30° of the largest polygon's center. Preserves contiguous
    # archipelagos (Japan, Philippines, Indonesia) but drops overseas
    # territories that distort the country's apparent footprint.
    if len(raw_polys) > 1:
        main_poly = max(raw_polys, key=lambda p: p.area)
        mx0, my0, mx1, my1 = main_poly.bounds
        mc_x = (mx0 + mx1) / 2
        mc_y = (my0 + my1) / 2
        filtered = []
        for p in raw_polys:
            px0, py0, px1, py1 = p.bounds
            pc_x = (px0 + px1) / 2
            pc_y = (py0 + py1) / 2
            if abs(pc_x - mc_x) <= 30 and abs(pc_y - mc_y) <= 30:
                filtered.append(p)
        if filtered:
            raw_polys = filtered

    # Project each polygon
    proj_series = gpd.GeoSeries(raw_polys, crs=world_gdf.crs).to_crs(target_crs)
    polys = list(proj_series)

    # ── Figure ─────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(16, 9), dpi=100, facecolor=CREAM)

    # Title — two-part typography matching state cards
    title_prefix = "How The New York Times Covers "
    country_token = country.upper()
    PREFIX_FS = 36
    COUNTRY_FS = 44
    y_title = 0.905

    inv = fig.transFigure.inverted()
    p_temp = fig.text(0, 0, title_prefix, fontsize=PREFIX_FS,
                      family='serif', weight='semibold')
    s_temp = fig.text(0, 0, country_token, fontsize=COUNTRY_FS,
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
    country_artist = fig.text(start_x + pw, y_title, country_token,
                              fontsize=COUNTRY_FS, family='serif',
                              weight='bold', color=TITLE_BLUE,
                              ha='left', va='baseline')

    # Underline the country name
    fig.canvas.draw()
    sb2 = country_artist.get_window_extent()
    ux0 = inv.transform((sb2.x0, 0))[0]
    ux1 = inv.transform((sb2.x1, 0))[0]
    uy = inv.transform((0, sb2.y0))[1] - 0.006
    underline = plt.Line2D([ux0, ux1], [uy, uy],
                           color=TITLE_BLUE, linewidth=2.5,
                           transform=fig.transFigure,
                           solid_capstyle='round')
    fig.add_artist(underline)

    # Subhead
    fig.text(0.5, 0.838,
             f"Subjects in {country} that The New York Times covers most out "
             f"of proportion to international coverage include:",
             fontsize=18, family='serif', color='#4a4438',
             ha='center')

    # ── Country outline ────────────────────────────────────────────────
    country_ax = fig.add_axes([0.03, 0.10, 0.42, 0.66])
    country_ax.set_facecolor(CREAM)
    country_ax.set_aspect('equal')
    country_ax.axis('off')

    # Drop shadow
    for poly in polys:
        if not poly.exterior:
            continue
        xs, ys = poly.exterior.xy
        rng_x = max(xs) - min(xs)
        rng_y = max(ys) - min(ys)
        ox = rng_x * 0.010
        oy = rng_y * 0.010
        country_ax.fill([x + ox for x in xs], [y - oy for y in ys],
                        facecolor='#bdb39d', edgecolor='none',
                        alpha=0.55, zorder=1)

    # Main fill
    for poly in polys:
        if not poly.exterior:
            continue
        xs, ys = poly.exterior.xy
        country_ax.fill(xs, ys, facecolor=COUNTRY_FILL, edgecolor='none', zorder=1.5)

    # Crisp outline
    for poly in polys:
        if not poly.exterior:
            continue
        xs, ys = poly.exterior.xy
        country_ax.plot(xs, ys, color=INK, linewidth=2.5, solid_joinstyle='round',
                        zorder=3)

    # Capital star
    cap = COUNTRY_CAPITALS.get(country)
    if cap is not None:
        _, lon, lat = cap
        cap_proj = (gpd.GeoSeries([Point(lon, lat)], crs=world_gdf.crs)
                    .to_crs(target_crs).iloc[0])
        country_ax.plot([cap_proj.x], [cap_proj.y], marker='*', color='#a83a2c',
                        markersize=14, markeredgecolor='#5a1a0e',
                        markeredgewidth=0.8, zorder=4)

    # Auto-scale
    all_x = [c for p in polys if p.exterior for c in p.exterior.xy[0]]
    all_y = [c for p in polys if p.exterior for c in p.exterior.xy[1]]
    xmin, xmax = min(all_x), max(all_x)
    ymin, ymax = min(all_y), max(all_y)
    pad_x = (xmax - xmin) * 0.06
    pad_y = (ymax - ymin) * 0.06
    country_ax.set_xlim(xmin - pad_x, xmax + pad_x)
    country_ax.set_ylim(ymin - pad_y, ymax + pad_y)

    # ── Theme list ─────────────────────────────────────────────────────
    items = recurring[:n_themes]
    n = len(items)
    if n == 0:
        fig.text(0.48, 0.40, '(no qualifying themes)',
                 fontsize=18, family='serif', color=MUTED, style='italic')
    else:
        # Larger themes need more vertical space; shrink the font when there
        # are many so the stat line still fits beneath each title.
        title_fs = 24 if n <= 3 else (22 if n == 4 else 20)
        stat_fs = 12 if n <= 4 else 11
        stat_gap = 0.030 if n <= 4 else 0.026
        y_top = 0.72 if n >= 5 else 0.70
        y_bot = 0.18 if n >= 5 else 0.20
        if n == 1:
            ys = [(y_top + y_bot) / 2]
        else:
            ys = [y_top - i * (y_top - y_bot) / (n - 1) for i in range(n)]

        for t, y in zip(items, ys):
            disp = display_name(t['tag'], country)
            fig.text(0.48, y, disp,
                     fontsize=title_fs, weight='bold', family='serif', color=INK)
            stats = (f"{int(round(t['score']))}× as common as in international coverage  ·  "
                     f"{fmt_pct(t['pct'])} of {country} articles")
            fig.text(0.48, y - stat_gap, stats,
                     fontsize=stat_fs, family='serif', color=MUTED)

    # Methods footer
    fig.text(0.5, 0.055,
             "Methods: Analysis of The New York Times-assigned keywords in "
             "World coverage between 2000–2026"
             "  ·  Data from NYT Archive API"
             "  ·  tedalcorn.github.io/nyt",
             fontsize=14, ha='center', family='serif', color='#7a7368')

    plt.savefig(output_path, dpi=100, facecolor=CREAM)
    plt.close()
    return True


def main():
    targets = sys.argv[1:] if len(sys.argv) > 1 else None

    print('Loading country geometries…')
    geo_path = os.path.join(PROJECT_DIR, 'data', 'world_countries.geojson')
    world_gdf = gpd.read_file(geo_path)
    print(f'  {len(world_gdf)} country polygons')

    print('Running theme analysis…')
    arts = load_world_articles()
    res = analyze(arts)
    print(f'  {len(res)} countries scored')

    # Cards are organized by region. Europe is the only region currently
    # populated; South America / Africa & Middle East / Asia & Oceania will
    # follow in their own subfolders.
    region = os.environ.get('REGION', 'Europe')
    out_dir = os.path.join(PROJECT_DIR, 'outputs', '2026-05-top-keyword',
                           '2026-05-13-world-country-tweets',
                           'countries', region)
    os.makedirs(out_dir, exist_ok=True)

    # Per-region country lists (analysis names). 'Britain' is the NYT-side
    # form for the United Kingdom; 'Czech Republic' is the NYT form for
    # Czechia.
    REGION_COUNTRIES = {
        'Europe': {
            'Albania', 'Andorra', 'Austria', 'Belarus', 'Belgium',
            'Bosnia and Herzegovina', 'Bulgaria', 'Croatia', 'Cyprus',
            'Czech Republic', 'Czechia', 'Denmark', 'Estonia', 'Finland',
            'France', 'Germany', 'Great Britain', 'Britain', 'Greece',
            'Hungary', 'Iceland', 'Ireland', 'Italy', 'Kosovo', 'Latvia',
            'Liechtenstein', 'Lithuania', 'Luxembourg', 'Macedonia', 'Malta',
            'Moldova', 'Monaco', 'Montenegro', 'Netherlands',
            'North Macedonia', 'Norway', 'Poland', 'Portugal', 'Romania',
            'Russia', 'San Marino', 'Serbia', 'Slovakia', 'Slovenia',
            'Spain', 'Sweden', 'Switzerland', 'Turkey', 'Ukraine', 'Vatican',
        },
    }
    region_filter = REGION_COUNTRIES.get(region, set())

    # Countries excluded from card generation. Should mirror the same
    # exclusions the regional maps apply:
    #   - United States: 9/11 spillover dominates its World-section tags
    #   - Albania: top tag 'Sociology' is 3 articles from a single 2008
    #     feature series — not a recurring pattern
    #   - Cyprus: top tag 'Palestinians' (score 4.4×, 6 articles) is the
    #     2002 Bethlehem siege exiles — one-event cluster, not recurring
    SKIP_COUNTRIES = {'United States', 'Albania', 'Cyprus'}

    # Minimum overrepresentation score required for a card. Matches the
    # MIN_SCORE_TO_LABEL=6.0 the Europe map uses to drop weak signals.
    MIN_SCORE_FOR_CARD = 6.0

    if targets is None:
        valid_keys = [c for c in res.keys() if c is not None]
        targets = [c for c in sorted(valid_keys) if c not in SKIP_COUNTRIES]
        if region_filter:
            targets = [c for c in targets if c in region_filter]
        # Drop countries whose strongest recurring signal is below threshold
        targets = [
            c for c in targets
            if res.get(c, {}).get('recurring')
            and res[c]['recurring'][0]['score'] >= MIN_SCORE_FOR_CARD
        ]
    else:
        targets = [c for c in targets if c not in SKIP_COUNTRIES or c in sys.argv[1:]]

    for country in targets:
        if country not in res:
            print(f'  Skipping {country} — no analysis result')
            continue
        recurring = res[country]['recurring']
        tag_years = res[country].get('tag_years', {})
        out_path = os.path.join(out_dir, f'{slugify(country)}.png')
        ok = make_card(country, recurring, out_path, world_gdf,
                       tag_years=tag_years)
        if ok:
            print(f'  ✓ {out_path}')


if __name__ == '__main__':
    main()
