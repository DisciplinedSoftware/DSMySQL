#!/usr/bin/env bash
# scripts/format.sh  —  run clang-format on all project source files.
#
# Usage:
#   ./scripts/format.sh [--check]
#
# Options:
#   --check   Verify formatting without modifying files (exits non-zero if
#             any file would be changed — useful in CI).

set -euo pipefail

die()  { echo "error: $*" >&2; exit 1; }
info() { echo "  →  $*"; }

CHECK_ONLY=0
if [[ "${1:-}" == "--check" ]]; then
    CHECK_ONLY=1
fi

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# Directories that contain project source (build/ is intentionally excluded).
SOURCE_DIRS=(
    "$REPO_ROOT/lib"
    "$REPO_ROOT/tests"
    "$REPO_ROOT/examples"
)

command -v clang-format &>/dev/null \
    || die "clang-format not found — install it and make sure it is on PATH"

mapfile -t FILES < <(
    find "${SOURCE_DIRS[@]}" \
        \( -name "*.cpp" -o -name "*.hpp" -o -name "*.h" -o -name "*.c" \) \
        -print | sort
)

[[ ${#FILES[@]} -gt 0 ]] || { info "No source files found."; exit 0; }

if [[ $CHECK_ONLY -eq 1 ]]; then
    info "Checking formatting of ${#FILES[@]} file(s) …"
    UNFORMATTED=()
    for f in "${FILES[@]}"; do
        if ! clang-format --dry-run --Werror "$f" &>/dev/null; then
            UNFORMATTED+=("$f")
        fi
    done
    if [[ ${#UNFORMATTED[@]} -gt 0 ]]; then
        echo "The following file(s) are not correctly formatted:"
        for f in "${UNFORMATTED[@]}"; do
            echo "  $f"
        done
        exit 1
    fi
    info "All files are correctly formatted."
else
    info "Formatting ${#FILES[@]} file(s) …"
    clang-format -i "${FILES[@]}"
    info "Done."
fi
