#!/usr/bin/env bash
#
# Run the four backfills together, one log each. ~55 min.
#
#     caffeinate -is bash deploy/run_backfill.sh
#
# Any sdp.backfill flag passes through (--dry-run, --limit 5, --force). A rerun
# retries only the gaps, because ingest() skips published partitions. Stop the
# launchd agent first, or two runs race on the same vendor files:
#
#     launchctl bootout gui/$(id -u)/com.sdp.daily
#     launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.sdp.daily.plist
#
set -u -o pipefail

# Independent of the working directory.
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

# Use the venv interpreter, not `uv run`: concurrent `uv run` calls race on the venv.
PY="$REPO/.venv/bin/python"
if [ ! -x "$PY" ]; then
    echo "The venv interpreter is absent: $PY" >&2
    echo "Run: uv sync" >&2
    exit 1
fi

# The S3 flat files roll on a five-year window, so the floor is today minus five
# years, computed and never written down.
BARS_START="${BARS_START:-$(date -v-5y +%Y-%m-%d)}"

# Short volume has a real coverage floor at 2024-02-06, not the rolling window.
SHORT_VOLUME_START="${SHORT_VOLUME_START:-2024-02-06}"

# Yesterday. Session D's flat file lands ~05:00 UTC on D+1, so a failure on the
# newest date only means the vendor has not published it yet.
END="${END:-$(date -v-1d +%Y-%m-%d)}"

LOGS="$REPO/data/_logs"
mkdir -p "$LOGS"

# Refuse a second run while one is in progress. `mkdir` is a lock and a test in
# one step. Two concurrent runs once failed 288 of 1,255 dates.
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

# "$@" passes the caller's flags through.
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
