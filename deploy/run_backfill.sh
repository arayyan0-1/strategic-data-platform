#!/usr/bin/env bash
#
# Run the four backfills together, one log for each.
#
#     caffeinate -is bash deploy/run_backfill.sh
#
# caffeinate keeps the Mac awake. The run takes about 55 minutes, which is set by
# the slowest of the four and not by their total.
#
# Pass any flag of sdp.backfill and every target receives it:
#
#     bash deploy/run_backfill.sh --dry-run      # count the sessions and stop
#     bash deploy/run_backfill.sh --limit 5      # test the first five dates
#
# Nothing needs a rerun by hand. ingest() skips a partition that is already
# published, so running this script again retries only the gaps.
#
# Stop the launchd agent first. It fires at 09:00 on the same targets, and two
# processes that reach the same date write the same .part file in vendor/.
#
#     launchctl bootout gui/$(id -u)/com.sdp.daily
#     launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.sdp.daily.plist
#
set -u -o pipefail

# The script must not depend on the working directory. That error happened twice
# already with the Python paths.
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

# Use the interpreter of the venv and not `uv run`. Four concurrent `uv run`
# calls each rebuild and reinstall the package, and they race on one venv.
PY="$REPO/.venv/bin/python"
if [ ! -x "$PY" ]; then
    echo "The venv interpreter is absent: $PY" >&2
    echo "Run: uv sync" >&2
    exit 1
fi

# The S3 flat files sit on a rolling five-year window. Measured on 2026-08-23:
# 2021-08-20 is absent and 2021-08-23 is present, which is that date minus five
# years exactly. The floor therefore moves forward every day, so it is computed
# and never written down. See docs/decisions/0014.
BARS_START="${BARS_START:-$(date -v-5y +%Y-%m-%d)}"

# Short volume has a real coverage floor and it is not the rolling window.
# 2023-06-01 returns nothing and 2024-02-06 returns records.
SHORT_VOLUME_START="${SHORT_VOLUME_START:-2024-02-06}"

# Yesterday. The flat file for session D lands at about 05:00 UTC on D+1, so
# today's file does not exist yet. A failure on the newest date means only that
# the vendor has not published it. Run the script again and it is picked up.
END="${END:-$(date -v-1d +%Y-%m-%d)}"

LOGS="$REPO/data/_logs"
mkdir -p "$LOGS"

# Refuse a second run while one is in progress. `mkdir` either creates the
# directory or fails, in one step, so it is a lock and a test at the same time.
#
# This exists because it happened. On 2026-08-23 a second copy of this script ran
# beside the first and 288 of 1,255 ticker dates failed. Unique temporary names
# now make that harmless rather than destructive, but two runs still fetch every
# date twice and neither finishes sooner.
LOCK="$LOGS/.backfill.lock"
if ! mkdir "$LOCK" 2>/dev/null; then
    running=$(cat "$LOCK/pid" 2>/dev/null || echo "unknown")
    echo "A backfill already holds the lock: $LOCK" >&2
    echo "It was started by pid $running." >&2
    if [ "$running" != "unknown" ] && ! kill -0 "$running" 2>/dev/null; then
        echo "That process is gone, so the lock is stale. Remove it with:" >&2
        echo "    rm -rf $LOCK" >&2
    fi
    exit 1
fi
echo "$$" > "$LOCK/pid"
trap 'rm -rf "$LOCK"' EXIT

echo "repo   $REPO"
echo "bars   $BARS_START to $END"
echo "short  $SHORT_VOLUME_START to $END"
echo "logs   $LOGS"
echo

declare -a NAMES=()
declare -a PIDS=()

# "$@" carries any flag that the caller passed, such as --dry-run.
"$PY" -m sdp.backfill day_aggs       "$BARS_START"         "$END" "$@" >"$LOGS/bf_day_aggs.log"       2>&1 &
NAMES+=("day_aggs");       PIDS+=("$!")
"$PY" -m sdp.backfill tickers        "$BARS_START"         "$END" "$@" >"$LOGS/bf_tickers.log"        2>&1 &
NAMES+=("tickers");        PIDS+=("$!")
"$PY" -m sdp.backfill short_volume   "$SHORT_VOLUME_START" "$END" "$@" >"$LOGS/bf_short_volume.log"   2>&1 &
NAMES+=("short_volume");   PIDS+=("$!")
"$PY" -m sdp.backfill short_interest "$BARS_START"         "$END" "$@" >"$LOGS/bf_short_interest.log" 2>&1 &
NAMES+=("short_interest"); PIDS+=("$!")

for i in "${!NAMES[@]}"; do
    echo "started ${NAMES[$i]} (pid ${PIDS[$i]}) -> $LOGS/bf_${NAMES[$i]}.log"
done
echo
echo "Watch one with:  tail -f $LOGS/bf_day_aggs.log"
echo

failed=0
for i in "${!NAMES[@]}"; do
    if wait "${PIDS[$i]}"; then
        echo "OK      ${NAMES[$i]}"
    else
        echo "FAILED  ${NAMES[$i]}  (see $LOGS/bf_${NAMES[$i]}.log)"
        failed=$((failed + 1))
    fi
done

echo
"$PY" -m sdp.dal
echo

if [ "$failed" -ne 0 ]; then
    echo "$failed of ${#NAMES[@]} targets reported a failed date."
    echo "Run this script again. It retries only the gaps."
    exit 1
fi
echo "All four targets finished with no failed date."
echo "Load the daily agent again:"
echo "    launchctl bootstrap gui/\$(id -u) ~/Library/LaunchAgents/com.sdp.daily.plist"
