# NYT Archive Dashboard

An interactive, self-updating dashboard built on the [NYT Archive API](https://developer.nytimes.com/docs/archive-product/1/overview). Tracks article counts, section trends, author stats, geographic coverage, and subject beats across 25+ years of New York Times content.

**Live demo:** [tedalcorn.github.io/nyt](https://tedalcorn.github.io/nyt)

---

## What it does

- **Overview** — annual and monthly article counts, section breakdown, byline counts over time
- **Sections** — per-section volume trends, top authors by section
- **Authors** — searchable author profiles with article timelines, beats, and co-author networks
- **Beats** — subject keyword trends with TF-IDF scoring to surface topic emphasis
- **World** — geographic coverage heatmap and country-level trend charts
- **States** — U.S. state coverage trends
- **People & Orgs** — searchable index of people and organizations mentioned in articles

Data is refreshed monthly via a GitHub Actions workflow.

---

## Using this as a template

This project is built entirely on the free [NYT Archive API](https://developer.nytimes.com/docs/archive-product/1/overview), which returns metadata (headline, byline, section, keywords, URL) for every article published in a given month. The dashboard itself is a single static HTML file with no server required — it runs entirely in the browser from pre-built JSON files.

To adapt this for your own outlet or API source:

1. **Replace `fetch_nyt.py`** with a script that fetches your data and writes monthly JSON files to `data/raw/`. Each file should contain a list of article objects with the fields your `build_data.py` reads.

2. **Adapt `build_data.py`** to parse your article format. The key output files are:
   - `data/articles_YYYY.json` — one per year, compact article records
   - `data/authors.json` — aggregated author statistics
   - `data/dashboard.json` — pre-computed chart data
   - `data/beats.json` — subject beat data
   - `data/subjects.json` — people and organizations index

3. **Deploy to GitHub Pages** — push the repo to GitHub, enable Pages in repository settings (source: root of `main` branch), and the dashboard at `index.html` serves automatically.

---

## Setup

### Prerequisites

- Python 3.9+
- An NYT Archive API key (free at [developer.nytimes.com](https://developer.nytimes.com/))

### Install dependencies

```bash
pip install requests
```

The choropleth map scripts (`make_choropleth.py`, `make_world_choropleth.py`) additionally require `geopandas` and `matplotlib`, but these are optional — the dashboard does not depend on the static map images.

### Fetch data

```bash
export NYT_API_KEY=your_key_here
python fetch_nyt.py
```

This downloads one JSON file per month into `data/raw/`, starting from January 2000. Only months not yet downloaded are fetched (incremental). The NYT API rate limit is 10 requests/minute; the script sleeps 6 seconds between requests.

### Build the dashboard data

```bash
python build_data.py
```

Processes all raw files and writes the JSON files in `data/` that the dashboard reads. Takes a few minutes on a full run (~2.2M articles).

### View locally

Open `index.html` in a browser. No server needed — all data is loaded from the `data/` directory via relative paths.

---

## Automated monthly updates

### GitHub Actions (recommended)

The workflow in `.github/workflows/update.yml` runs on the 2nd of each month. Add your API key as a repository secret named `NYT_API_KEY` in **Settings → Secrets and variables → Actions**.

### Local cron / launchd (macOS)

`nightly_update.sh` fetches new data, rebuilds, and pushes to GitHub. Store your API key in `~/.nyt_api_key` (one line, no quotes), then schedule it with cron or a launchd plist.

---

## File reference

| File | Purpose |
|------|---------|
| `fetch_nyt.py` | Downloads raw monthly JSON from the NYT Archive API |
| `build_data.py` | Processes raw data into dashboard JSON files |
| `update.py` | Orchestrates fetch + build in one command |
| `nightly_update.sh` | Shell wrapper for local scheduled updates |
| `index.html` | The dashboard (single-file, no build step) |
| `make_choropleth.py` | Generates U.S. state choropleth PNGs (optional) |
| `make_world_choropleth.py` | Generates world choropleth PNGs (optional) |
| `.github/workflows/update.yml` | Monthly GitHub Actions update workflow |
| `data/raw/` | Raw monthly API responses (gitignored, ~3GB) |
| `data/articles_YYYY.json` | Per-year compact article records |
| `data/authors.json` | Author statistics and metadata |
| `data/dashboard.json` | Pre-computed chart data |
| `data/beats.json` | Subject beat data with TF-IDF scores |
| `data/subjects.json` | People and organizations index |

---

## Notes on the data

The NYT Archive API returns metadata only — not full article text. Coverage begins in 1851 but this dashboard starts at 2000, where metadata quality is consistent. A few known quirks are documented in the "About the data" section of the dashboard itself.

The `build_data.py` script includes logic to deduplicate author name variants (e.g. "Jane Smith" vs "Jane A. Smith"), normalize section names across the archive's history, and handle API parsing edge cases. These are NYT-specific but the patterns are common to any long-running archive.
