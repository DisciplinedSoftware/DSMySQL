#!/usr/bin/env bash
# scripts/act-ci.sh  —  run CI tests-gcc locally with act using a free MySQL host port.
#
# Usage:
#   bash ./scripts/act-ci.sh
#   bash ./scripts/act-ci.sh --pull=false

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
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
echo "Running only tests-gcc job in act"

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

act push \
  -W "$TMP_WORKFLOW" \
  -j tests-gcc \
  -P ubuntu-latest=catthehacker/ubuntu:act-latest \
  --env DEBIAN_FRONTEND=noninteractive \
  --env NEEDRESTART_MODE=a \
  "$@"
