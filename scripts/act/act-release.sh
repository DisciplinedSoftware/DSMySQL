#!/usr/bin/env bash
# scripts/act/act-release.sh  —  run release workflow locally with act
#
# Usage:
#   ./scripts/act/act-release.sh
#
# What it does:
#   1. Extracts the current version from CMakeLists.txt.
#   2. Creates a simulated GitHub webhook event for a tag push.
#   3. Runs `act` with that event to test the release workflow locally.

set -euo pipefail

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

die()  { echo "error: $*" >&2; exit 1; }
info() { echo "  →  $*"; }

# ──────────────────────────────────────────────────────────────────────────────
# Extract version from CMakeLists.txt
# ──────────────────────────────────────────────────────────────────────────────

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CMAKE_FILE="$REPO_ROOT/CMakeLists.txt"

cd "$REPO_ROOT"

VERSION="$(grep -m1 'project(DSMySQL' "$CMAKE_FILE" -A5 \
  | grep 'VERSION' \
  | grep -oP '\d+\.\d+\.\d+' || true)"

[[ -n "$VERSION" ]] || die "Could not parse VERSION from CMakeLists.txt"
info "Extracted version: $VERSION"

# ──────────────────────────────────────────────────────────────────────────────
# Create temporary event file
# ──────────────────────────────────────────────────────────────────────────────

EVENT_FILE=$(mktemp)
trap "rm -f '$EVENT_FILE'" EXIT

cat > "$EVENT_FILE" <<EOF
{
  "ref": "refs/tags/v${VERSION}",
  "base_ref": "main",
  "before": "0000000000000000000000000000000000000000",
  "after": "$(git rev-parse HEAD)",
  "repository": {
    "full_name": "DisciplinedSoftware/DSMySQL",
    "name": "DSMySQL",
    "owner": {
      "login": "DisciplinedSoftware"
    }
  },
  "pusher": {
    "name": "local-tester",
    "email": "test@local"
  }
}
EOF

info "Created event file: $EVENT_FILE"
info "Running: act push -e $EVENT_FILE"

# ──────────────────────────────────────────────────────────────────────────────
# Run act with the simulated event
# ──────────────────────────────────────────────────────────────────────────────

act push -e "$EVENT_FILE"
