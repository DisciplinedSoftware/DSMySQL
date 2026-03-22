#!/usr/bin/env bash
# scripts/ci/act-ci-full.sh  —  run all Linux CI jobs locally with act, sequentially.
#
# Jobs run in order:
#   1) tests-gcc
#   2) tests-clang
#   3) package-consumer-smoke
#
# Usage:
#   bash ./scripts/ci/act-ci-full.sh
#   bash ./scripts/ci/act-ci-full.sh --pull=false

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

find_free_port() {
  local port=3306
  while ss -H -ltn "sport = :${port}" | grep -q .; do
    port=$((port + 1))
  done
  echo "$port"
}

MYSQL_PORT="$(find_free_port)"
echo "Using MySQL host port: ${MYSQL_PORT}"
echo "Running Linux CI jobs sequentially with act"

TMP_WORKFLOW="$(mktemp --suffix=.ci.act.yml)"
trap 'rm -f "$TMP_WORKFLOW"' EXIT

awk '
  BEGIN { skip=0 }
  /^      - name: Export mapped MySQL service port$/ { skip=1; next }
  {
    if (skip == 1) {
      if ($0 ~ /^      - name: /) {
        skip=0
      } else {
        next
      }
    }
    print
  }
' .github/workflows/ci.yml > "$TMP_WORKFLOW"

sed -i "s/DS_MYSQL_TEST_PORT: 3306/DS_MYSQL_TEST_PORT: ${MYSQL_PORT}/" "$TMP_WORKFLOW"
sed -i "s/- 3306\/tcp/- ${MYSQL_PORT}:3306/" "$TMP_WORKFLOW"

run_job() {
  local job="$1"
  shift
  echo
  echo "=== Running ${job} ==="
  act push \
    -W "$TMP_WORKFLOW" \
    -j "$job" \
    -P ubuntu-latest=catthehacker/ubuntu:act-latest \
    --env DEBIAN_FRONTEND=noninteractive \
    --env NEEDRESTART_MODE=a \
    "$@"
}

run_job tests-gcc "$@"
run_job tests-clang "$@"
run_job package-consumer-smoke "$@"

echo
echo "All Linux CI jobs completed successfully."
