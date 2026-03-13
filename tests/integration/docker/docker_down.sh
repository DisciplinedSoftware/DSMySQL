#!/bin/bash
set -e

ENV_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../.env"
if [[ -f "$ENV_FILE" ]]; then
  set -a
  . "$ENV_FILE"
  set +a
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../../.."
COMPOSE_FILE="$ROOT_DIR/tests/integration/docker/docker-compose.yml"
PORT_FILE="$ROOT_DIR/build/.mysql_port"

# Read the port that was used during setup
if [[ -f "$PORT_FILE" ]]; then
  export DS_MYSQL_TEST_PORT=$(cat "$PORT_FILE")
  echo "Using port from setup: $DS_MYSQL_TEST_PORT"
  rm -f "$PORT_FILE"
fi

cd "$ROOT_DIR"
echo "Stopping MySQL container..."
docker-compose -f "$COMPOSE_FILE" down -v || true

echo "MySQL stopped"
