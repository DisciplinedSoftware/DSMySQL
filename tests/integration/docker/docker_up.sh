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

# Check whether a TCP port is already bound on the host.
# Tries ss, then lsof, then a Python socket bind as a fallback.
port_in_use() {
  local port=$1
  if command -v ss >/dev/null 2>&1; then
    ss -tlnp 2>/dev/null | grep -qE ":${port}[[:space:]]"
  elif command -v lsof >/dev/null 2>&1; then
    lsof -i ":${port}" >/dev/null 2>&1
  else
    python3 -c "
import socket, sys
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
try:
    s.bind(('', int(sys.argv[1])))
    s.close()
    sys.exit(1)   # port was free
except OSError:
    sys.exit(0)   # port is in use
" "$port" 2>/dev/null
  fi
}

# Find an available port if not already set
if [[ -z "$DS_MYSQL_TEST_PORT" ]]; then
  PORT=3307
  while port_in_use "$PORT"; do
    echo "Port $PORT is in use, trying next port..."
    ((PORT++))
  done
  export DS_MYSQL_TEST_PORT=$PORT
  echo "Using available port: $DS_MYSQL_TEST_PORT"
else
  echo "Using configured port: $DS_MYSQL_TEST_PORT"
fi

# Ensure the directory for the port file exists and save the port for
# docker_down.sh and run_tests_wrapper.sh to read at test runtime.
mkdir -p "$(dirname "$PORT_FILE")"
echo "$DS_MYSQL_TEST_PORT" > "$PORT_FILE"

cd "$ROOT_DIR"
echo "Starting MySQL container on port $DS_MYSQL_TEST_PORT..."
docker-compose -f "$COMPOSE_FILE" up -d

echo "Waiting for MySQL to be ready..."
for i in {1..60}; do
  if docker-compose -f "$COMPOSE_FILE" exec -T mysql mysqladmin ping -h localhost > /dev/null 2>&1; then
    echo "MySQL is ready"
    exit 0
  fi
  echo "Waiting... ($i/60)"
  sleep 1
done

echo "MySQL failed to start"
exit 1
