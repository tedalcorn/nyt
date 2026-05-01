#!/usr/bin/env python3
"""Full-stack update orchestrator.

Pulls fresh NYT API data, rebuilds the dashboard, refreshes obituaries +
corrections + denominators, then runs validate.py to surface suspicious
records (parser drift, scrape gaps, low-word-count outliers) the user
should eyeball before pushing.

Usage (run from the project root):
  python update.py                # Fetch new + rebuild everything
  python update.py --rebuild      # Skip the API fetch; rebuild from data/raw
  python update.py --no-corr      # Skip the corrections scrape (faster nightly)
  python update.py --no-validate  # Skip the validation pass

Each step is wrapped in a STEP printout so failures are easy to localize. If
a step fails, the orchestrator stops — better to surface a problem than to
silently push partial data.
"""
import sys, subprocess, time, os
from datetime import date

# Always run from the project root so relative paths (data/, cache/) resolve correctly
ROOT = os.path.dirname(os.path.abspath(__file__))
os.chdir(ROOT)
S = os.path.join(ROOT, 'scripts')  # scripts/ subfolder


def step(label, cmd):
    print(f'\n━━━ {label} ━━━')
    print(f'    $ {" ".join(cmd)}')
    t0 = time.time()
    try:
        subprocess.run(cmd, check=True, cwd=ROOT)
    except subprocess.CalledProcessError as e:
        print(f'\nFAILED: {label} (exit {e.returncode})')
        sys.exit(e.returncode)
    print(f'    ({time.time() - t0:.1f}s)')


def s(name):
    """Return path to a script in the scripts/ subfolder."""
    return os.path.join(S, name)


def main():
    args = set(sys.argv[1:])
    do_fetch    = '--rebuild' not in args
    do_corr     = '--no-corr' not in args
    do_validate = '--no-validate' not in args
    cur_year = str(date.today().year)
    py = sys.executable

    if do_fetch:
        step('Fetching new months from NYT Archive API', [py, s('fetch_nyt.py')])

    step('Rebuilding dashboard data',                    [py, s('build_data.py')])
    step('Rebuilding unique reporters (section + state)',[py, s('build_unique_reporters.py')])
    step('Rebuilding obituaries',                        [py, s('build_obituaries.py')])
    step('Applying surgical obit fixes',                 [py, s('regenerate_obit_interactive_fixes.py')])

    if do_corr:
        step(f'Scraping corrections ({cur_year})',       [py, s('scrape_corrections.py'), cur_year])
        step('Building corrections matched + augmented', [py, s('build_corrections.py')])
        step('Re-applying inline URL matches',           [py, '-c',
            'import json; cm=json.load(open("data/corrections_matched.json")); '
            '[c.update(match_url=c["inline_url"],match_source="inline_url",match_score=99) '
            'for c in cm if not c.get("match_url") and c.get("inline_url")]; '
            'json.dump(cm,open("data/corrections_matched.json","w"),ensure_ascii=False,separators=(",",":"))'
        ])
        step('Rebuilding corrections denominators',      [py, s('build_corrections_denominators.py')])

    if do_validate:
        step('Validating fresh records',                 [py, s('validate.py')])

    print('\nAll steps complete. Remember to update the timestamp in index.html and push.')


if __name__ == '__main__':
    main()
