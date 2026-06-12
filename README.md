# NYT Archive Dashboard

An interactive, self-updating dashboard built on the [NYT Archive API](https://developer.nytimes.com/docs/archive-product/1/overview). Tracks article counts, section trends, reporter and newsmaker profiles, geographic coverage, subject beats, obituaries, and corrections across 25+ years of New York Times content.

**Live:** [tedalcorn.github.io/nyt](https://tedalcorn.github.io/nyt)

---

## What the dashboard shows

The site is a single static page (`index.html`) that loads pre-built JSON from `data/` and renders everything in the browser. Since the 2026-06-12 v2 adoption, heavy data loads on demand: the Headlines tab fetches packed per-year files (`data/v2/`) on first open, and full article files load the first time a detail panel needs them — nothing row-level downloads at page-open. Tabs across the top:

| Tab | What it shows |
|-----|---|
| **Beats** | Subject keyword trends scored by TF-IDF to surface real topic emphasis, with merge/filter rules in `data/tag_config.json` |
| **Headlines** | Searchable headline tracker across the full archive |
| **Newsmakers** | People and organizations mentioned in articles (renamed from "Subjects" 2026-05-13) |
| **Sections** | Per-section volume trends and top reporters by section |
| **Reporters** | Searchable reporter profiles with timelines, beats, co-author networks, staff/freelance designation, and bio links where available |
| **U.S. States** | State-level coverage trends with a 50-state choropleth, recurring themes vs. headline events per state, and a downloadable Twitter-thread graphic |
| **World** | Country-level coverage with regional maps (Europe, Americas, Africa+Middle East, Asia+Oceania) and per-country recurring themes |
| **Obituaries** | Obituary database with longest-obit chart, by-year mode, and a portrait-format infographic of the longest obits 2000–present |
| **Corrections** | Corrections tracker with denominators (corrections per page, per article) and inline-URL matching to the corrected article |
| **About** | Methodology notes |

---

## Data pipeline

```
NYT Archive API
       │
       ▼
fetch_nyt.py          # one JSON per month into data/raw/  (~317 months, gitignored)
       │
       ▼
build_data.py         # core build: articles_YYYY.json, authors.json, beats.json, dashboard.json
build_v2_tracker.py   # packs tracker_YYYY.json into data/v2/ (one string per year; only the packed form is deployed)
build_unique_reporters.py
build_obituaries.py + regenerate_obit_interactive_fixes.py
scrape_corrections.py + build_corrections.py + build_corrections_denominators.py
validate.py           # quality checks before push
```

`update.py` orchestrates all of the above in the right order. Run it from the repo root with `python update.py`. Useful flags: `--rebuild` (skip API fetch), `--no-corr` (skip corrections scrape), `--no-validate`.

---

## Auto-update

Daily rebuild + push runs locally via macOS launchd at 12:00 PM Mountain Time:

- **Plist:** `~/Library/LaunchAgents/com.ted.nyt-update.plist`
- **Wrapper:** `~/scripts/nyt_nightly_update.sh` (lives outside the repo because of macOS TCC rules on `~/Desktop/`)
- **Push helper:** `~/scripts/commit_and_push_nyt.py` (runs under pyenv-python so git inherits Desktop TCC access)
- **Logs:** `~/Library/Logs/nyt-nightly-update.log`

The wrapper sets `PROJECT_DIR=/Users/tedalcorn/Desktop/claude-projects/nyt/site` and runs `update.py` from there.

Cloud-based updates via GitHub Actions (`.github/workflows/update.yml`) are currently **disabled** — the workflow remains in the repo as a fallback but won't fire on schedule. Re-enable with `gh workflow enable "Update NYT Data"` if needed.

---

## Repo layout

```
site/                              ← this repo (deployed to GitHub Pages from main branch root)
├── index.html                     # the entire dashboard
├── review_corrections.html        # admin page for correction review
├── below-the-fold-logo.png        # site logo
├── update.py                      # build orchestrator
├── data/                          # pre-built JSON consumed by index.html
│   ├── articles_YYYY.json         # per-year article records (compact)
│   ├── authors.json               # reporter profiles
│   ├── beats.json                 # subject keyword data
│   ├── dashboard.json             # pre-computed chart data
│   ├── tag_config.json            # subject merges, filters, headline-event rules
│   ├── raw/                       # raw monthly API JSON (gitignored, ~3 GB)
│   └── bio_photos/                # uncompressed source illustrations (gitignored)
├── graphics/
│   ├── below-the-fold-logo.png    # site logo
│   ├── state-map.jpg / .png       # downloadable U.S. states graphic
│   ├── final-obits.jpg / .png     # downloadable longest-obits graphic
│   └── bio_photos/                # served bio photos (manifest.json + Compressed Illustrations/)
├── scripts/                       # ONGOING build pipeline only (one-off project scripts live in the parent's projects/ folder)
│   ├── fetch_nyt.py               # API fetcher
│   ├── build_data.py              # core dashboard builder
│   ├── build_unique_reporters.py  # reporter lists by section + state
│   ├── build_obituaries.py + regenerate_obit_interactive_fixes.py
│   ├── scrape_corrections.py + build_corrections.py + build_corrections_denominators.py
│   ├── patch_*.py                 # surgical patches (run when their input source changes — e.g. patch_beats after tag_config.json edits)
│   ├── scrape_bios.py             # run periodically to refresh reporter bios
│   ├── validate.py
│   └── archive/                   # old/deprecated scripts kept for reference
└── cache/                         # scrape caches (gitignored, resumable)
```

The parent folder one level up (`/Users/tedalcorn/Desktop/claude-projects/nyt/`) is the local working area and is NOT part of this repo. It holds:
- `projects/` — analyses and products derived from the dashboard. Each project has its own `scripts/` folder when it has build scripts (state-map, regional maps, doodles, etc.). The `corrections-workflow/` sub-project contains the tools and review page used to manually match NYT corrections to articles.
- `logos/` — evolving logo source versions (the deployed copy lives in `site/graphics/`).

---

## Data caveats

- The Archive API has a 2–4 week indexing lag for the current month; `fetch_nyt.py` re-fetches the current month plus the two prior on every run to backfill.
- The API returns metadata only, not full article text. Word counts come from the `word_count` field where present; pre-2007 long-form articles are paginated and the API records page 1 only, so a few historical pieces have understated word counts.
- January 2025 the Archive API renamed `persons → Person`, `organizations → Organization`, `subject → Subject`, `glocations → Location`, `creative_works → Title`. The ingest accepts both old and new schemas.
- Reporter name deduplication is automatic for prefix/middle-initial variants; nickname/formal pairs and other irregularities are listed in `data/author_overrides.json`.
- Corrections coverage has gaps where the NYT pageoneplus page is DataDome-walled and not crawled by the Wayback Machine; affected URLs are listed in `data/corrections_save_todo.md` for hand-saving.

---

## Re-running locally

```bash
cd /Users/tedalcorn/Desktop/claude-projects/nyt/site
python update.py                    # full pipeline (~25 min including corrections scrape)
python update.py --rebuild          # skip API fetch
python update.py --no-corr          # skip corrections scrape (faster)
```

API key: store the Archive API key in `~/.nyt_api_key` (one line, no quotes). For the Article Search API (used by `automatch_corrections.py`), use `~/.nyt_search_api_key`.

After `update.py` finishes, push manually with `git add data/ && git commit -m "..." && git push`, or let the launchd job do it overnight.
