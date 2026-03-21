#!/usr/bin/env bash
# scripts/release.sh  —  bump version, update CHANGELOG, commit, and tag.
#
# Usage:
#   ./scripts/release.sh <new-version>
#   ./scripts/release.sh 1.2.0
#
# What it does:
#   1. Validates the supplied version string.
#   2. Ensures the working tree is clean and on `main`.
#   3. Updates the VERSION field in CMakeLists.txt.
#   4. Opens CHANGELOG.md in $EDITOR so you can fill in the release notes.
#   5. Commits the two changed files.
#   6. Creates an annotated tag.
#   7. Optionally pushes branch + tag to origin.

set -euo pipefail

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

die()  { echo "error: $*" >&2; exit 1; }
info() { echo "  →  $*"; }

# ──────────────────────────────────────────────────────────────────────────────
# Argument check
# ──────────────────────────────────────────────────────────────────────────────

[[ $# -eq 1 ]] || die "Usage: $0 <new-version>  (e.g. 1.2.0)"

NEW_VERSION="$1"
[[ "$NEW_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] \
    || die "Version must be in MAJOR.MINOR.PATCH format, got '$NEW_VERSION'"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CMAKE_FILE="$REPO_ROOT/CMakeLists.txt"
CHANGELOG="$REPO_ROOT/CHANGELOG.md"

# ──────────────────────────────────────────────────────────────────────────────
# Git sanity checks
# ──────────────────────────────────────────────────────────────────────────────

cd "$REPO_ROOT"

CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
[[ "$CURRENT_BRANCH" == "main" ]] \
    || die "Must be on the 'main' branch (currently on '$CURRENT_BRANCH')"

[[ -z "$(git status --porcelain)" ]] \
    || die "Working tree is not clean — commit or stash your changes first"

# Make sure we have the latest remote state
git fetch --quiet origin

LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main 2>/dev/null || echo "")
[[ -z "$REMOTE" || "$LOCAL" == "$REMOTE" ]] \
    || die "Local main is not up to date with origin/main — run 'git pull' first"

# Reject if tag already exists
git tag | grep -q "^v${NEW_VERSION}$" \
    && die "Tag v${NEW_VERSION} already exists"

# ──────────────────────────────────────────────────────────────────────────────
# Read current version from CMakeLists.txt
# ──────────────────────────────────────────────────────────────────────────────

CURRENT_VERSION="$(grep -oP '(?<=VERSION )\d+\.\d+\.\d+' "$CMAKE_FILE" | head -1)"
[[ -n "$CURRENT_VERSION" ]] || die "Could not parse current VERSION from CMakeLists.txt"
info "Current version : $CURRENT_VERSION"
info "New version     : $NEW_VERSION"

[[ "$NEW_VERSION" != "$CURRENT_VERSION" ]] \
    || die "New version ($NEW_VERSION) is the same as the current version"

# ──────────────────────────────────────────────────────────────────────────────
# Bump version in CMakeLists.txt
# ──────────────────────────────────────────────────────────────────────────────

info "Updating CMakeLists.txt …"
# Replace the first occurrence of the version number inside the project() block.
sed -i "0,/VERSION ${CURRENT_VERSION}/s/VERSION ${CURRENT_VERSION}/VERSION ${NEW_VERSION}/" \
    "$CMAKE_FILE"

# Verify the substitution
UPDATED_VERSION="$(grep -oP '(?<=VERSION )\d+\.\d+\.\d+' "$CMAKE_FILE" | head -1)"
[[ "$UPDATED_VERSION" == "$NEW_VERSION" ]] \
    || die "CMakeLists.txt update failed — expected $NEW_VERSION, got $UPDATED_VERSION"

# ──────────────────────────────────────────────────────────────────────────────
# Update CHANGELOG.md
# ──────────────────────────────────────────────────────────────────────────────

TODAY="$(date +%Y-%m-%d)"

# Insert a new dated section after the [Unreleased] heading
info "Inserting CHANGELOG section for v${NEW_VERSION} …"
sed -i \
    "s|## \[Unreleased\]|## [Unreleased]\n\n---\n\n## [${NEW_VERSION}] – ${TODAY}|" \
    "$CHANGELOG"

# Update or insert the comparison link for the new version.
# Assumes the previous version link already exists (e.g. from the initial 1.0.0 release).
if grep -q "^\[Unreleased\]:" "$CHANGELOG"; then
    sed -i \
        "s|^\[Unreleased\]:.*|[Unreleased]: https://github.com/DisciplinedSoftware/DSMySQL/compare/v${NEW_VERSION}...HEAD|" \
        "$CHANGELOG"
fi

# Add the new version comparison link if it doesn't already exist
if ! grep -q "^\[${NEW_VERSION}\]:" "$CHANGELOG"; then
    sed -i \
        "/^\[Unreleased\]:/a [${NEW_VERSION}]: https://github.com/DisciplinedSoftware/DSMySQL/compare/v${CURRENT_VERSION}...v${NEW_VERSION}" \
        "$CHANGELOG"
fi

# Open CHANGELOG in $EDITOR so the author can fill in the release notes
EDITOR="${EDITOR:-vi}"
info "Opening CHANGELOG.md in ${EDITOR} …"
"$EDITOR" "$CHANGELOG"

# ──────────────────────────────────────────────────────────────────────────────
# Commit and tag
# ──────────────────────────────────────────────────────────────────────────────

info "Staging changed files …"
git add "$CMAKE_FILE" "$CHANGELOG"

git diff --cached --stat

echo ""
read -rp "Proceed with commit and tag v${NEW_VERSION}? [y/N] " CONFIRM
[[ "${CONFIRM,,}" == "y" ]] || { echo "Aborted."; exit 0; }

info "Committing …"
git commit -m "chore: release v${NEW_VERSION}"

info "Tagging v${NEW_VERSION} …"
git tag -a "v${NEW_VERSION}" -m "DSMySQL v${NEW_VERSION}"

echo ""
echo "Commit and tag created locally."
echo ""
read -rp "Push branch and tag to origin? [y/N] " PUSH_CONFIRM
if [[ "${PUSH_CONFIRM,,}" == "y" ]]; then
    info "Pushing …"
    git push origin main "v${NEW_VERSION}"
    echo ""
    echo "Done. The release workflow will start automatically on GitHub."
else
    echo ""
    echo "Not pushed. When ready, run:"
    echo "  git push origin main v${NEW_VERSION}"
fi
