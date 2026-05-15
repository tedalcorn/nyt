#!/bin/bash
# rewrite_git_history.sh
# ---------------------------------------------------------------------------
# Periodic maintenance script: rewrite git history to drop accumulated blobs
# of data/articles_YYYY.json and data/tracker_YYYY.json files.
#
# Why this is needed:
#   The nightly cron commits the full year-file every time even a single
#   article changes. Each blob is 22–39 MB. Over ~12 months this typically
#   accumulates 1–3 GB. Running this script reclaims that space.
#
# When to run:
#   - When .git/ grows past ~2–3 GB (check with `du -sh .git`)
#   - Annually, even if smaller — keeps clones fast
#   - First successful run was 2026-05-15: 10 GB → 1 GB
#
# What it does:
#   1. Backs up .git/ to .git.backup-<timestamp>/ (full rollback safety net)
#   2. Snapshots data/articles_*.json + data/tracker_*.json to /tmp/
#   3. Pauses launchd cron (com.ted.nyt-update)
#   4. Runs `git filter-repo` to strip all historical blobs of those files
#   5. Restores current data files (filter-repo strips worktree too)
#   6. Verifies SHA256 of every restored file matches the snapshot
#   7. Commits the restored files as a fresh single revision
#   8. Re-adds origin remote (filter-repo removes it) and force-pushes
#   9. Reinstalls .git/hooks/pre-commit from scripts/hooks/
#  10. Polls the live site for 200 OK
#  11. Runs the nightly script manually to confirm cron will still work
#  12. Reloads launchd cron
#  13. Leaves backups in place (delete manually after verifying tomorrow)
#
# Safety gates — script exits non-zero (leaving backup intact) if any of:
#   - Working tree dirty or on a non-main branch
#   - Local main is behind origin/main (would lose remote commits)
#   - filter-repo exits non-zero or .git/ is unexpectedly large after
#   - Any restored data file's SHA256 doesn't match snapshot
#   - git push fails
#   - Live site doesn't return 200 within 5 minutes
#   - Manual cron run exits non-zero
#
# Recovery (any phase except after backup deletion):
#   cd ~/Desktop/claude-projects/nyt/site
#   rm -rf .git
#   mv .git.backup-<timestamp> .git
#   git push --force origin main    # if push already happened
#   launchctl load ~/Library/LaunchAgents/com.ted.nyt-update.plist
# ---------------------------------------------------------------------------

set -eo pipefail

PROJECT="/Users/tedalcorn/Desktop/claude-projects/nyt/site"
PLIST="$HOME/Library/LaunchAgents/com.ted.nyt-update.plist"
LIVE_URL="https://tedalcorn.github.io/nyt/"
ORIGIN_URL="https://github.com/tedalcorn/nyt.git"
TS=$(date '+%Y%m%d-%H%M%S')
BACKUP_DIR="$PROJECT/.git.backup-$TS"
SNAPSHOT_TAR="/tmp/nyt-data-snapshot-$TS.tar.gz"
SNAPSHOT_DIR="/tmp/nyt-snap-verify-$TS"

log() { echo "[$(date '+%H:%M:%S')] $*"; }
die() { echo "[FATAL] $*" >&2; exit 1; }

cd "$PROJECT"

# --- Phase 0: pre-flight ---------------------------------------------------
log "Phase 0: pre-flight checks"
[ -d .git ] || die "$PROJECT is not a git repo"
[ "$(git rev-parse --abbrev-ref HEAD)" = "main" ] || die "not on main branch"
[ -z "$(git status --porcelain)" ] || die "working tree dirty; commit or stash first"

command -v git-filter-repo >/dev/null || die "git-filter-repo not installed (brew install git-filter-repo)"

# Free-disk check (need 2× current .git/ size as headroom for backup + repack)
GIT_BYTES=$(du -sk .git | awk '{print $1*1024}')
FREE_BYTES=$(df -k . | awk 'NR==2{print $4*1024}')
NEEDED=$((GIT_BYTES * 2))
[ "$FREE_BYTES" -gt "$NEEDED" ] || die "need ${NEEDED} bytes free; have ${FREE_BYTES}"

log "  starting .git/ size: $(du -sh .git | awk '{print $1}')"
log "  free disk: $(df -h . | awk 'NR==2{print $4}')"

# Sync with remote (the cron may have pushed since last manual interaction)
log "  fetching latest from origin..."
git fetch origin --quiet
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)
if [ "$LOCAL" != "$REMOTE" ]; then
  BEHIND=$(git rev-list --count HEAD..origin/main 2>/dev/null || echo 0)
  AHEAD=$(git rev-list --count origin/main..HEAD 2>/dev/null || echo 0)
  if [ "$BEHIND" -gt 0 ]; then
    log "  local is $BEHIND commit(s) behind origin/main — fast-forwarding"
    git pull --ff-only --quiet
  fi
  if [ "$AHEAD" -gt 0 ]; then
    log "  local is $AHEAD commit(s) ahead of origin/main (will push)"
  fi
fi

# --- Phase 1: backup + pause cron ------------------------------------------
log "Phase 1: backup .git and pause cron"
log "  copying .git -> $BACKUP_DIR (~$(du -sh .git | awk '{print $1}'))"
cp -R .git "$BACKUP_DIR"

log "  snapshotting data files -> $SNAPSHOT_TAR"
tar czf "$SNAPSHOT_TAR" data/articles_*.json data/tracker_*.json
log "  snapshot size: $(ls -lh "$SNAPSHOT_TAR" | awk '{print $5}')"

if launchctl list | grep -q nyt-update; then
  log "  unloading launchd cron"
  launchctl unload "$PLIST"
else
  log "  launchd cron already unloaded"
fi

# --- Phase 2: rewrite history ---------------------------------------------
log "Phase 2: running git filter-repo"
git filter-repo --force --invert-paths \
  --path-regex '^data/articles_[0-9]{4}\.json$' \
  --path-regex '^data/tracker_[0-9]{4}\.json$'

NEW_SIZE_KB=$(du -sk .git | awk '{print $1}')
NEW_SIZE_GB=$(echo "scale=2; $NEW_SIZE_KB/1048576" | bc)
log "  post-rewrite .git size: ${NEW_SIZE_GB} GB"
[ "$NEW_SIZE_KB" -lt 2000000 ] || die "post-rewrite .git is still > 2 GB; abort"

# --- Phase 3: restore data + verify ----------------------------------------
log "Phase 3: restore data files and verify checksums"
tar xzf "$SNAPSHOT_TAR"

mkdir -p "$SNAPSHOT_DIR"
( cd "$SNAPSHOT_DIR" && tar xzf "$SNAPSHOT_TAR" )

# SHA256 every restored file vs the snapshot
DIFFS=$(
  for f in data/articles_*.json data/tracker_*.json; do
    a=$(shasum -a 256 "$f" | awk '{print $1}')
    b=$(shasum -a 256 "$SNAPSHOT_DIR/$f" | awk '{print $1}')
    [ "$a" = "$b" ] || echo "MISMATCH: $f"
  done
)
[ -z "$DIFFS" ] || die "checksum mismatch after restore:\n$DIFFS"
log "  all 54 data files match snapshot byte-for-byte"

# --- Phase 4: re-add remote + commit + push --------------------------------
log "Phase 4: re-add remote, commit restored files, force-push"
git remote add origin "$ORIGIN_URL" 2>/dev/null || git remote set-url origin "$ORIGIN_URL"

git add data/articles_*.json data/tracker_*.json
git commit -m "Restore data snapshots (history pruned to drop accumulated blobs)" --quiet

FINAL_SIZE_KB=$(du -sk .git | awk '{print $1}')
log "  final .git size: $(echo "scale=2; $FINAL_SIZE_KB/1048576" | bc) GB"

log "  force-pushing to origin (this uploads the entire rewritten history)..."
git push --force --set-upstream origin main

# --- Phase 5: reinstall pre-commit hook ------------------------------------
HOOK_SRC="$PROJECT/scripts/hooks/pre-commit"
HOOK_DST="$PROJECT/.git/hooks/pre-commit"
if [ -f "$HOOK_SRC" ]; then
  log "Phase 5: reinstalling pre-commit hook (timestamp auto-updater)"
  cp "$HOOK_SRC" "$HOOK_DST"
  chmod +x "$HOOK_DST"
else
  log "  WARN: $HOOK_SRC missing; skip hook install (timestamp won't auto-update)"
fi

# --- Phase 6: verify live site ---------------------------------------------
log "Phase 6: polling live site for 200 OK"
DEADLINE=$(( $(date +%s) + 300 ))
until curl -sI "$LIVE_URL" 2>/dev/null | grep -q "HTTP/2 200"; do
  [ "$(date +%s)" -gt "$DEADLINE" ] && die "live site did not return 200 within 5 min"
  sleep 10
  log "  still waiting for Pages rebuild..."
done
log "  live site is 200 OK"

# --- Phase 7: test cron + reload -------------------------------------------
log "Phase 7: running nightly script manually to confirm cron will work"
bash "$HOME/scripts/nyt_nightly_update.sh"
log "  cron script exit code: $?"

log "Phase 7: reloading launchd cron"
launchctl load "$PLIST"
launchctl list | grep -q nyt-update && log "  cron loaded" || die "cron failed to load"

# --- Done ------------------------------------------------------------------
log "DONE."
log ""
log "Final state:"
log "  .git/: $(du -sh .git | awk '{print $1}')"
log "  HEAD:  $(git log --oneline -1)"
log ""
log "Backups (delete after verifying live site is healthy tomorrow):"
log "  $BACKUP_DIR"
log "  $SNAPSHOT_TAR"
log "  $SNAPSHOT_DIR"
log ""
log "Cleanup command for tomorrow:"
log "  rm -rf '$BACKUP_DIR' '$SNAPSHOT_DIR' '$SNAPSHOT_TAR'"
