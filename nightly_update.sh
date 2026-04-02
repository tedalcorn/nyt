#!/bin/bash
# Nightly NYT dashboard update.
# Fetches any unfetched months (including historical back to 2000), rebuilds
# dashboard data, then commits and pushes so GitHub Pages stays current.
#
# Setup:
#   1. Store your NYT API key in ~/.nyt_api_key  (one line, no quotes)
#   2. Load the launchd agent:
#        launchctl load ~/Library/LaunchAgents/com.ted.nyt-update.plist
#   3. To test manually: bash nightly_update.sh

set -e
cd /Users/tedalcorn/Desktop/claude-projects/nyt

# Load API key from dedicated secrets file (not tracked in git)
if [ -f "$HOME/.nyt_api_key" ]; then
  export NYT_API_KEY="$(cat "$HOME/.nyt_api_key")"
fi

if [ -z "$NYT_API_KEY" ]; then
  echo "$(date): ERROR — NYT_API_KEY not set. Store it in ~/.nyt_api_key" >> nightly_update.log
  exit 1
fi

echo "$(date): Starting nightly update..." >> nightly_update.log

/usr/bin/python3 update.py >> nightly_update.log 2>&1

echo "$(date): Build complete." >> nightly_update.log

# Commit and push if there are changes to the data directory
if git rev-parse --git-dir > /dev/null 2>&1; then
  if ! git diff --quiet data/ || git ls-files --others --exclude-standard data/ | grep -q .; then
    git add data/
    git commit -m "Auto-update: $(date '+%Y-%m-%d')" >> nightly_update.log 2>&1
    git push >> nightly_update.log 2>&1
    echo "$(date): Pushed to GitHub." >> nightly_update.log
  else
    echo "$(date): No data changes to commit." >> nightly_update.log
  fi
fi

echo "$(date): Done." >> nightly_update.log
