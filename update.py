#!/usr/bin/env python3
"""Full-stack update orchestrator.

Pulls fresh NYT API data, rebuilds the dashboard, refreshes obituaries +
corrections + denominators, then runs validate.py to surface suspicious
records (parser drift, scrape gaps, low-word-count outliers) the user
should eyeball before pushing.

Usage:
  python update.py                # Fetch new + rebuild everything
  python update.py --rebuild      # Skip the API fetch; rebuild from data/raw
  python update.py --no-corr      # Skip the corrections scrape (faster nightly)
  python update.py --no-validate  # Skip the validation pass

Each step is wrapped in a STEP printout so failures are easy to localize. If
a step fails, the orchestrator stops — better to surface a problem than to
silently push partial data.
"""
import sys, subprocess, time
from datetime import date


def step(label, cmd):
    print(f'\n━━━ {label} ━━━')
    print(f'    $ {" ".join(cmd)}')
    t0 = time.time()
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f'\nFAILED: {label} (exit {e.returncode})')
        sys.exit(e.returncode)
    print(f'    ({time.time() - t0:.1f}s)')


def main():
    args = set(sys.argv[1:])
    do_fetch    = '--rebuild' not in args
    do_corr     = '--no-corr' not in args
    do_validate = '--no-validate' not in args
    cur_year = str(date.today().year)
    py = sys.executable

    if do_fetch:
        step('Fetching new months from NYT Archive API', [py, 'fetch_nyt.py'])

    step('Rebuilding dashboard data (build_data.py)', [py, 'build_data.py'])

    step('Rebuilding obituaries', [py, 'build_obituaries.py'])
    # Apply surgical name/profession overrides + drop NON_OBIT_URLs that the
    # full builder already handles, but the regenerator also catches anything
    # added since the last full build. Idempotent.
    step('Applying surgical obit fixes', [py, 'regenerate_obit_interactive_fixes.py'])

    if do_corr:
        # Only re-scrape the current year for nightly runs; historical pages
        # are cached and don't change.
        step(f'Scraping corrections (year: {cur_year})',
             [py, 'scrape_corrections.py', cur_year])
        step('Building corrections matched + augmented',
             [py, 'build_corrections.py'])
        step('Rebuilding corrections denominators',
             [py, 'build_corrections_denominators.py'])

    if do_validate:
        step('Validating fresh records', [py, 'validate.py'])

    print('\nAll steps complete.')


if __name__ == '__main__':
    main()
