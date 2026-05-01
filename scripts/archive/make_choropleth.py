#!/usr/bin/env python3
"""
Generate US state coverage choropleth maps (articles per million residents).
Outputs choropleth_allyears.png and choropleth_recent.png.
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

STATE_POP = {
    "Alabama": 5.024, "Alaska": 0.733, "Arizona": 7.151, "Arkansas": 3.012,
    "California": 39.538, "Colorado": 5.774, "Connecticut": 3.606, "Delaware": 0.990,
    "D.C.": 0.689, "Florida": 21.538, "Georgia": 10.712, "Hawaii": 1.456,
    "Idaho": 1.839, "Illinois": 12.812, "Indiana": 6.785, "Iowa": 3.190,
    "Kansas": 2.938, "Kentucky": 4.506, "Louisiana": 4.658, "Maine": 1.362,
    "Maryland": 6.178, "Massachusetts": 7.030, "Michigan": 10.077,
    "Minnesota": 5.707, "Mississippi": 2.961, "Missouri": 6.155, "Montana": 1.085,
    "Nebraska": 1.961, "Nevada": 3.104, "New Hampshire": 1.378, "New Jersey": 9.289,
    "New Mexico": 2.117, "New York": 20.201, "North Carolina": 10.439,
    "North Dakota": 0.779, "Ohio": 11.799, "Oklahoma": 3.960, "Oregon": 4.238,
    "Pennsylvania": 13.003, "Rhode Island": 1.098, "South Carolina": 5.119,
    "South Dakota": 0.887, "Tennessee": 6.911, "Texas": 29.146, "Utah": 3.272,
    "Vermont": 0.643, "Virginia": 8.632, "Washington": 7.706,
    "West Virginia": 1.794, "Wisconsin": 5.894, "Wyoming": 0.577,
}

HATCHED_STATES = {"New York", "New Jersey", "Connecticut"}

FIPS_NAME = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "DC": "D.C.", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine",
    "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska",
    "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico",
    "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
    "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
}


def load_state_totals():
    with open(os.path.join(DATA_DIR, "dashboard.json")) as f:
        dash = json.load(f)
    cov = dash["us_state_coverage"]
    all_years = [str(y) for y in dash["all_years"]]
    recent_years = [y for y in all_years if int(y) >= 2021]
    return cov["state_trends"], all_years, recent_years


def sum_years(trends, years):
    return {s: sum(yd.get(y, 0) for y in years) for s, yd in trends.items()}


def make_map(title, per_capita, gdf, clip_val, outfile):
    cmap = plt.cm.YlOrRd
    norm = mcolors.Normalize(vmin=0, vmax=clip_val)

    fig = plt.figure(figsize=(14, 8.5))
    fig.patch.set_facecolor('#f7f5f0')

    ax = fig.add_axes([0.01, 0.06, 0.87, 0.87])
    ax_ak = fig.add_axes([0.01, 0.05, 0.22, 0.26])
    ax_hi = fig.add_axes([0.23, 0.05, 0.14, 0.16])
    for a in (ax, ax_ak, ax_hi):
        a.set_facecolor('#f7f5f0')
        a.axis('off')

    def plot_region(ax_target, subset):
        df = subset.copy()
        df["_val"] = df["state_name"].map(lambda s: min(per_capita.get(s, 0) or 0, clip_val))
        has_data = df[df["state_name"].isin(per_capita)]
        no_data = df[~df["state_name"].isin(per_capita)]
        if len(has_data):
            has_data.plot(column="_val", cmap=cmap, norm=norm, ax=ax_target,
                         edgecolor="white", linewidth=0.5)
        if len(no_data):
            no_data.plot(ax=ax_target, color="#dddddd", edgecolor="white", linewidth=0.5)
        hatched = df[df["state_name"].isin(HATCHED_STATES)]
        if len(hatched):
            hatched.plot(ax=ax_target, facecolor="none", edgecolor="#555",
                        linewidth=0.4, hatch="////", alpha=0.55)

    cont = gdf[~gdf["state_name"].isin(["Alaska", "Hawaii"])]
    plot_region(ax, cont)
    plot_region(ax_ak, gdf[gdf["state_name"] == "Alaska"])
    plot_region(ax_hi, gdf[gdf["state_name"] == "Hawaii"])

    # Place * annotation on the map near NY/NJ/CT (northeast, EPSG:5070 coords)
    ax.annotate('*', xy=(1820000, 2180000), fontsize=20, color='#222',
                fontweight='bold', ha='center', va='center')

    # Colorbar
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cax = fig.add_axes([0.90, 0.2, 0.015, 0.55])
    cb = fig.colorbar(sm, cax=cax)
    cb.set_label("Articles per million residents", fontsize=9, labelpad=8)
    ticks = np.linspace(0, clip_val, 6)
    cb.set_ticks(ticks)
    cb.set_ticklabels([f"{int(v):,}" + ("+" if v == clip_val else "") for v in ticks])
    cb.ax.tick_params(labelsize=8)

    ax.set_title(title, fontsize=13, fontweight='bold', pad=8)
    fig.text(0.47, 0.01,
             "* Hatched states have additional coverage in the \"New York\" section, not included in these totals.",
             ha='center', fontsize=8, color='#666', style='italic')

    plt.savefig(outfile, dpi=150, bbox_inches='tight', facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"Saved {outfile}")


def main():
    print("Loading coverage data...")
    state_trends, all_years, recent_years = load_state_totals()

    totals_all = sum_years(state_trends, all_years)
    totals_recent = sum_years(state_trends, recent_years)

    per_capita_all = {s: totals_all[s] / STATE_POP[s]
                      for s in totals_all if s in STATE_POP}
    per_capita_recent = {s: totals_recent[s] / STATE_POP[s]
                         for s in totals_recent if s in STATE_POP}

    print(f"All years ({all_years[0]}–{all_years[-1]}) top 5:")
    for s, v in sorted(per_capita_all.items(), key=lambda x: -x[1])[:5]:
        print(f"  {s}: {v:.0f}/M")
    print(f"Recent years ({recent_years[0]}–{recent_years[-1]}) top 5:")
    for s, v in sorted(per_capita_recent.items(), key=lambda x: -x[1])[:5]:
        print(f"  {s}: {v:.0f}/M")

    print("Loading shapefile...")
    gdf = gpd.read_file(os.path.join(DATA_DIR, "cb_2023_us_state_20m.shp")).to_crs("EPSG:5070")
    gdf["state_name"] = gdf["STUSPS"].map(FIPS_NAME)

    make_map(
        f"NYT U.S. Coverage by State ({all_years[0]}–{all_years[-1]})\n"
        "Articles in U.S. section · per million residents · total",
        per_capita_all, gdf, clip_val=600,
        outfile=os.path.join(PROJECT_DIR, "choropleth_allyears.png")
    )
    make_map(
        f"NYT U.S. Coverage by State ({recent_years[0]}–{recent_years[-1]})\n"
        "Articles in U.S. section · per million residents · total",
        per_capita_recent, gdf, clip_val=250,
        outfile=os.path.join(PROJECT_DIR, "choropleth_recent.png")
    )


if __name__ == "__main__":
    main()
