#!/usr/bin/env bash
# Write the launchd agents for this checkout. Usage: deploy/install_agents.sh [DEST_DIR]
set -euo pipefail

repo="$(cd "$(dirname "$0")/.." && pwd)"
dest="${1:-$HOME/Library/LaunchAgents}"
mkdir -p "$dest" "$repo/data/_logs"

for label in com.sdp.daily com.sdp.dashboard; do
    sed "s|__REPO__|$repo|g" "$repo/deploy/$label.plist" > "$dest/$label.plist"
    plutil -lint -s "$dest/$label.plist"
    echo "wrote $dest/$label.plist"
done

cat <<MSG

Load them (bootout first if they're already loaded):
  launchctl bootstrap gui/\$(id -u) $dest/com.sdp.daily.plist
  launchctl bootstrap gui/\$(id -u) $dest/com.sdp.dashboard.plist
MSG
