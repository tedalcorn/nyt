# Corrections scraping gap report

Generated: 2026-05-10. Live CDX probing was throttled by Wayback's rate
limiter mid-audit; classifications below are from the file inventory plus
prior direct testing. Re-run `scripts/audit_corrections_gaps.py` when
Wayback connectivity is healthy to refresh.

## Coverage in scope (2014+)

| Year | API total | Cached | Missing | Coverage |
|------|----------:|-------:|--------:|---------:|
| 2014 | 363 | 358 | 5 | 98.6% |
| 2015 | 343 | 333 | 10 | 97.1% |
| 2016 | 327 | 309 | 18 | 94.5% |
| 2017 | 305 | 294 | 11 | 96.4% |
| 2018 | 301 | 301 | 0 | 100.0% |
| 2019 | 332 | 332 | 0 | 100.0% |
| 2020 | 306 | 306 | 0 | 100.0% |
| 2021 | 295 | 295 | 0 | 100.0% |
| 2022 | 300 | 300 | 0 | 100.0% |
| 2023 | 301 | 300 | 1 | 99.7% |
| 2024 | 298 | 298 | 0 | 100.0% |
| 2025 | 272 | 231 | 41 | 84.9% |
| 2026 |  49 |   0 | 49 | 0.0% |

## Diagnosis by year

### 2018–2024: full coverage, no action needed.

### 2014–2017 (44 URLs missing across 4 years)
Old-era residual gaps. These predate the Wayback DataDome era and the
URLs almost certainly have 200-status snapshots in CDX. Either the
original scrape encountered transient failures and the cache was never
updated, or the API tagged a URL that never had a real corrections page.

**Action**: rerun `scripts/scrape_corrections.py 2014 2015 2016 2017`
under healthy Wayback connectivity. Many should resolve to `ok_cdx`.
Anything that returns `no_good_snapshot` after that retry is genuinely
unrecoverable from Wayback — verify by hand and either manually save
(if NYT still serves the URL) or accept the gap.

### 2023 (1 URL missing)
Same as 2014–2017: trivial, likely transient. One re-run resolves.

### 2025 (41 URLs missing) — most relevant to your current presentation
Date distribution:

| Month     | Missing |
|-----------|--------:|
| Feb 2025  |  3      |
| May 2025  |  2      |
| Jul 2025  |  1      |
| Oct 2025  |  1      |
| Nov 2025  | 18 ← Wayback indexing lag |
| Dec 2025  | 16 ← Wayback indexing lag |

**Pattern**: 34 of 41 (83%) are Nov–Dec 2025. Wayback's NYT indexing
typically lags by 1–3 months, so these are very likely **WAITING** —
they'll backfill on their own. The 7 scattered earlier-year ones (Feb
through Oct) are more concerning; they survived earlier scrape attempts.

**Action plan**:
1. **Wait 4–6 weeks**, re-run `scripts/scrape_corrections.py 2025`. The
   Nov–Dec ones should self-resolve.
2. After that, anything still missing in 2025 needs hand-classification
   (probably DataDome-walled — NYT enabled DataDome on /pageoneplus/
   somewhere mid-to-late 2025). Use `scripts/import_correction_html.py`
   to manually import.

### 2026 (49 URLs missing) — confirmed DataDome-walled
Direct testing earlier today showed every Wayback snapshot of these
URLs is a 403 from NYT's DataDome bot-protection. Wayback faithfully
archived the 403 page. **Will not resolve on its own.**

**Action**: hand-save when this period enters your presentation. Use
`scripts/import_correction_html.py <URL> <saved.html>`.

## Tooling

- `scripts/audit_corrections_gaps.py` — produces this report. Default
  scope is 2014+. Add `--no-probe-cdx` for the file-inventory-only fast
  pass (~10 seconds). The CDX-probe pass is slow (~1 min/URL when
  Wayback is healthy, often longer), and parallel probing trips
  Wayback rate limits — keep it serial.
- `scripts/import_correction_html.py <URL> <saved.html>` — drops a
  manually-saved HTML file into the scraper cache at the right slug.
  Sanity-checks for paywall / CAPTCHA pages. The next
  `build_corrections.py` run picks it up automatically.

## Suggested workflow going forward

1. After every `update.py` run, run the audit: `python scripts/audit_corrections_gaps.py`
2. Read the table. Anything in WAITING / HAS_200_RETRY → wait one cycle and re-scrape; should self-resolve.
3. Anything in DATADOME_WALLED → hand-save in browser → `python scripts/import_correction_html.py <URL> <path>` → re-run `build_corrections.py`.
4. The audit can be wired into `update.py` as a non-blocking final step if you want it run automatically.
