#!/bin/bash
# Nightly NYT dashboard update.
# Fetches any unfetched months (including historical back to 2000), rebuilds
# dashboard data, then commits and pushes so GitHub Pages stays current.
# Emails the address in NOTIFY_EMAIL env var on success/failure.
#
# Setup:
#   1. Store your NYT API key in ~/.nyt_api_key  (Archive API)
#   2. Optional: ~/.nyt_search_api_key (Article Search API, for automatch)
#   3. Load the launchd agent:
#        launchctl load ~/Library/LaunchAgents/com.ted.nyt-update.plist
#   4. To test manually: bash nightly_update.sh

set +e   # don't exit on first error — capture status and notify
cd "$(dirname "$0")"

LOG=nightly_update.log
NOTIFY_TO="$NOTIFY_EMAIL"
RUN_TS="$(date '+%Y-%m-%d %H:%M:%S %Z')"

# Load API keys from dedicated secrets files (not tracked in git)
if [ -f "$HOME/.nyt_api_key" ]; then
  export NYT_API_KEY="$(cat "$HOME/.nyt_api_key")"
fi
if [ -f "$HOME/.nyt_search_api_key" ]; then
  export NYT_SEARCH_API_KEY="$(cat "$HOME/.nyt_search_api_key")"
fi

send_email() {
  # $1 = subject, $2 = body. Uses Mail.app via osascript so we ride on
  # whatever account the user has configured (no SMTP credentials in the repo).
  local subj="$1"; local body="$2"
  /usr/bin/osascript <<APPLESCRIPT 2>>"$LOG" || true
tell application "Mail"
  set newMessage to make new outgoing message with properties {subject:"$subj", content:"$body", visible:false}
  tell newMessage
    make new to recipient at end of to recipients with properties {address:"$NOTIFY_TO"}
    send
  end tell
end tell
APPLESCRIPT
}

if [ -z "$NYT_API_KEY" ]; then
  echo "$RUN_TS: ERROR — NYT_API_KEY not set. Store it in ~/.nyt_api_key" >> "$LOG"
  send_email "[NYT update] FAILED — missing API key" "Run at $RUN_TS bailed: NYT_API_KEY not in environment. Store it in ~/.nyt_api_key."
  exit 1
fi

echo "$RUN_TS: Starting nightly update..." >> "$LOG"

# Run update.py and capture exit code
python3 update.py >> "$LOG" 2>&1
UPDATE_RC=$?

if [ $UPDATE_RC -ne 0 ]; then
  echo "$(date): update.py exited $UPDATE_RC" >> "$LOG"
  TAIL="$(tail -40 "$LOG" | sed 's/"/\\"/g')"
  send_email "[NYT update] FAILED at $RUN_TS" "update.py exited with status $UPDATE_RC.

Last 40 log lines:

$TAIL"
  exit $UPDATE_RC
fi

echo "$(date): Build complete." >> "$LOG"

# Commit and push if there are changes to the data directory
PUSH_NOTE="(no data changes to commit)"
if git rev-parse --git-dir > /dev/null 2>&1; then
  if ! git diff --quiet data/ || git ls-files --others --exclude-standard data/ | grep -q .; then
    git add data/ >> "$LOG" 2>&1
    git commit -m "Auto-update: $(date '+%Y-%m-%d')" >> "$LOG" 2>&1
    if git push >> "$LOG" 2>&1; then
      PUSH_NOTE="Pushed data update to GitHub."
      echo "$(date): Pushed to GitHub." >> "$LOG"
    else
      PUSH_NOTE="WARNING: data committed locally but git push failed (see log)."
      echo "$(date): git push failed." >> "$LOG"
    fi
  else
    echo "$(date): No data changes to commit." >> "$LOG"
  fi
fi

echo "$(date): Done." >> "$LOG"

# Pull the validate.py block out of the log for the email body
VALIDATE_BLOCK="$(awk '/Validating fresh data for/,/These are not blockers|No issues flagged/' "$LOG" | tail -80 | sed 's/"/\\"/g')"
[ -z "$VALIDATE_BLOCK" ] && VALIDATE_BLOCK="(no validation report found in log)"

send_email "[NYT update] OK at $RUN_TS" "Nightly update succeeded.

$PUSH_NOTE

Validation report:

$VALIDATE_BLOCK

Full log: $(pwd)/$LOG"
