#!/usr/bin/env python3
"""
Generate world coverage choropleth maps (articles per million residents per year).
Outputs choropleth_world_allyears.png and choropleth_world_recent.png.

Uses Natural Earth 110m countries shapefile (data/ne_world/).
"""

import json
import os
import geopandas as gpd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_DIR, "data")
NE_SHP = os.path.join(DATA_DIR, "ne_world", "ne_110m_admin_0_countries.shp")


# Map NYT keyword names → Natural Earth NAME field
# Only entries that differ from the NE name need to be listed.
NYT_TO_NE = {
    "Great Britain": "United Kingdom",
    "Gaza Strip": "Palestine",
    "West Bank": "Palestine",
    "East Jerusalem": "Palestine",
    "Czech Republic": "Czechia",
    "Ivory Coast": "Côte d'Ivoire",
    "DR Congo": "Dem. Rep. Congo",
    "Democratic Republic Of The Congo": "Dem. Rep. Congo",
    "Democratic Republic of Congo": "Dem. Rep. Congo",
    "Congo, Democratic Republic of (Congo-Kinshasa)": "Dem. Rep. Congo",
    "Republic Of Congo": "Congo",
    "Republic of Congo": "Congo",
    "Congo, Republic of (Congo-Brazzaville)": "Congo",
    "Congo, The Democratic Republic Of The": "Dem. Rep. Congo",
    "Bosnia": "Bosnia and Herz.",
    "Bosnia And Herzegovina": "Bosnia and Herz.",
    "Bosnia and Herzegovina": "Bosnia and Herz.",
    "Dominican Republic": "Dominican Rep.",
    "South Sudan": "S. Sudan",
    "Central African Republic": "Central African Rep.",
    "Equatorial Guinea": "Eq. Guinea",
    "Western Sahara": "W. Sahara",
    "East Timor": "Timor-Leste",
    "Macedonia": "North Macedonia",
    "Swaziland": "eSwatini",
    "Eswatini": "eSwatini",
    "United Arab Emirates": "United Arab Emirates",
    "Trinidad And Tobago": "Trinidad and Tobago",
    "Solomon Islands": "Solomon Is.",
    "Dem. Rep. Congo": "Dem. Rep. Congo",
    "Korea, South": "South Korea",
    "Korea, North": "North Korea",
}

# Locations that are not sovereign countries — skip for the map
NON_COUNTRY = {
    # US coverage is from the World section and is incomplete — shown with stripes
    "United States",
    # Continents / macro-regions
    "Europe", "Africa", "Middle East", "Asia", "Americas", "South America",
    "Central America", "Caribbean", "Caribbean Area", "Pacific Region",
    "Latin America", "Eastern Europe", "Central Asia",
    "Arctic Regions", "Antarctic Regions", "Antarctica",
    # Seas, oceans, waterways
    "Mediterranean", "Baltic Sea", "Adriatic Sea", "Aegean Sea",
    "Arabian Sea", "Atlantic Ocean", "Pacific Ocean", "Indian Ocean",
    "Arctic Ocean", "Red Sea", "Mediterranean Sea", "Black Sea",
    "Caspian Sea", "Persian Gulf", "Gulf Of Mexico", "Bering Sea",
    "South China Sea", "East China Sea", "Barents Sea", "English Channel",
    "Strait of Hormuz", "Gulf of Aden",
    # Geographic features, mountain ranges, rivers
    "Amazon River", "Amazon River Basin", "Amazon Jungle", "Nile River",
    "Alps Mountains", "Andes Mountains", "Himalayan Region", "Himalayas",
    "Ganges River", "Sahara Desert",
    # Disputed territories not shown as sovereign states on NE 110m
    "Kashmir", "Golan Heights", "Senkaku Islands", "Spratly Islands",
    "Paracel Islands", "Nagorno-Karabakh", "Korean Demilitarized Zone",
    # Sub-regions / multi-country descriptors
    "Balkans", "Balkan States", "Scandinavia", "Baltic Region",
    "Far East, South and Southeast Asia and Pacific Areas",
    "Far East", "Southeast Asia",
    # Cities / special administrative regions
    "Abu Ghraib", "Hong Kong", "Macau",
    # Historical / defunct states
    "Soviet Union", "Yugoslavia", "Czechoslovakia", "East Germany",
    "East Pakistan", "South Vietnam", "North Vietnam",
    # Sub-national regions
    "Northern Ireland", "Scotland", "Wales", "Corsica",
    "Chechnya", "Dagestan", "Tibet", "Xinjiang",
    # Territories
    "Puerto Rico", "Guam", "Virgin Islands", "Greenland", "Reunion",
}


def load_article_data():
    with open(os.path.join(DATA_DIR, "dashboard.json")) as f:
        dash = json.load(f)
    lt = dash["world_coverage"]["location_trends"]
    all_years = [y for y in dash["all_years"] if int(y) <= 2025]
    last_full_yr = max(int(y) for y in all_years)
    recent_years = [str(y) for y in range(last_full_yr - 4, last_full_yr + 1)]
    return lt, all_years, recent_years


def aggregate_totals(lt, years):
    """Sum articles per Natural Earth country name for the given years."""
    totals = {}
    for loc, by_year in lt.items():
        if loc in NON_COUNTRY:
            continue
        t = sum(by_year.get(y, 0) for y in years)
        if t == 0:
            continue
        ne_name = NYT_TO_NE.get(loc, loc)
        totals[ne_name] = totals.get(ne_name, 0) + t
    return totals


def make_map(title, per_m_per_year, gdf, bins, outfile):
    """
    bins: list of bin edges, e.g. [0, 0.5, 1, 2, 4, 8].
    The last value is treated as the cap; countries above it fall in the top bin.
    """
    cap = bins[-1]
    n_bins = len(bins) - 1

    cmap_base = plt.cm.YlOrRd
    colors = [cmap_base(i / (n_bins - 1)) for i in range(n_bins)]
    cmap = mcolors.ListedColormap(colors)
    norm = mcolors.BoundaryNorm(bins, cmap.N)

    fig, ax = plt.subplots(1, 1, figsize=(20, 11))
    fig.patch.set_facecolor('#f0ede8')
    ax.set_facecolor('#b8d8ea')  # ocean blue
    ax.set_axis_off()

    gdf2 = gdf.copy()

    USA_NE = "United States of America"

    def classify(name):
        if name == USA_NE:
            return None   # handled separately
        val = per_m_per_year.get(name)
        if val is None:
            return None
        return min(val, cap - 0.001)

    gdf2["_val"] = gdf2["NAME"].map(classify)

    usa_row    = gdf2[gdf2["NAME"] == USA_NE]
    no_data    = gdf2[(gdf2["_val"].isna()) & (gdf2["NAME"] != USA_NE)]
    has_data   = gdf2[gdf2["_val"].notna()]

    # Gray for no coverage
    no_data.plot(ax=ax, color="#cccccc", edgecolor="white", linewidth=0.3, zorder=1)
    # Choropleth for countries with data
    has_data.plot(column="_val", cmap=cmap, norm=norm, ax=ax,
                  edgecolor="white", linewidth=0.3, zorder=2)
    # US: neutral gray base + diagonal stripes (not measured in World section)
    if len(usa_row):
        usa_row.plot(ax=ax, color="#b0b0b0", edgecolor="white", linewidth=0.3, zorder=2)
        usa_row.plot(ax=ax, facecolor="none", edgecolor="#888888",
                     linewidth=0.5, hatch="////", zorder=3)

    # Colorbar (horizontal, at bottom)
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cax = fig.add_axes([0.15, 0.08, 0.70, 0.022])
    cb = fig.colorbar(sm, cax=cax, orientation='horizontal')
    cb.set_label("Articles per million residents per year (avg)", fontsize=10, labelpad=6)
    cb.set_ticks(bins)
    # Format tick labels: use decimal for values < 1, integer otherwise
    def fmt(v):
        return str(v) if v < 1 else str(int(v))
    tick_labels = [fmt(b) for b in bins[:-1]] + [f"{fmt(cap)}+"]
    cb.set_ticklabels(tick_labels, fontsize=9)

    # Legend swatches
    from matplotlib.patches import Patch, Rectangle
    import matplotlib.lines as mlines

    no_cov_patch = Patch(facecolor='#cccccc', edgecolor='white', label='No coverage')

    # US stripe swatch: a gray patch with hatch overlay
    us_patch = Patch(facecolor='#b0b0b0', edgecolor='#888888',
                     hatch='////', label='Not measured (U.S.)')

    ax.legend(handles=[no_cov_patch, us_patch],
              loc='lower left', fontsize=9, framealpha=0.8)

    ax.set_title(title, fontsize=13, fontweight='bold', pad=12)
    ax.set_xlim(-180, 180)
    ax.set_ylim(-57, 84)  # trim Antarctica

    plt.subplots_adjust(bottom=0.14)
    plt.savefig(outfile, dpi=150, bbox_inches='tight', facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"Saved {outfile}")


def main():
    lt, all_years, recent_years = load_article_data()

    print(f"Full period:   {all_years[0]}–{all_years[-1]} ({len(all_years)} years)")
    print(f"Recent period: {recent_years[0]}–{recent_years[-1]} ({len(recent_years)} years)")

    totals_full = aggregate_totals(lt, all_years)
    totals_recent = aggregate_totals(lt, recent_years)

    print(f"\nLoading shapefile from {NE_SHP}...")
    gdf = gpd.read_file(NE_SHP)

    # Population in millions from shapefile's POP_EST field
    pop_m = {}
    for _, row in gdf.iterrows():
        if row["POP_EST"] and row["POP_EST"] > 0:
            pop_m[row["NAME"]] = row["POP_EST"] / 1_000_000

    n_full = len(all_years)
    n_recent = len(recent_years)

    per_m_full = {}
    per_m_recent = {}

    for country, total in totals_full.items():
        pop = pop_m.get(country)
        if pop and pop >= 0.1:
            per_m_full[country] = (total / n_full) / pop

    for country, total in totals_recent.items():
        pop = pop_m.get(country)
        if pop and pop >= 0.1:
            per_m_recent[country] = (total / n_recent) / pop

    print("\nTop 20 full-period (articles/M/year):")
    for c, v in sorted(per_m_full.items(), key=lambda x: -x[1])[:20]:
        print(f"  {c:35} {v:6.1f}")

    print("\nTop 15 recent (articles/M/year):")
    for c, v in sorted(per_m_recent.items(), key=lambda x: -x[1])[:15]:
        print(f"  {c:35} {v:6.1f}")

    # ── Full period map ────────────────────────────────────────────────────
    # Log-ish bins tuned to the distribution (most countries 0–8, outliers above)
    make_map(
        f"NYT World Coverage · {all_years[0]}–{all_years[-1]}"
        "\nArticles per million residents per year (avg)  ·  World section only",
        per_m_full, gdf,
        bins=[0, 0.5, 1, 2, 4, 8],
        outfile=os.path.join(PROJECT_DIR, "choropleth_world_allyears.png"),
    )

    # ── Recent period map ──────────────────────────────────────────────────
    make_map(
        f"NYT World Coverage · {recent_years[0]}–{recent_years[-1]}"
        "\nArticles per million residents per year (avg)  ·  World section only",
        per_m_recent, gdf,
        bins=[0, 1, 2, 4, 8, 16],
        outfile=os.path.join(PROJECT_DIR, "choropleth_world_recent.png"),
    )


if __name__ == "__main__":
    main()
