#!/bin/bash
# Wrapper that resolves DS_MYSQL_TEST_PORT from the port file written by
# docker_up.sh when the port was selected dynamically at runtime.
# Used by CTest as the test command when SKIP_DOCKER_MANAGEMENT is OFF.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
PORT_FILE="$ROOT_DIR/build/.mysql_port"

if [[ -z "${DS_MYSQL_TEST_PORT}" ]] && [[ -f "$PORT_FILE" ]]; then
    export DS_MYSQL_TEST_PORT=$(<"$PORT_FILE")
    echo "run_tests_wrapper: using port $DS_MYSQL_TEST_PORT from $PORT_FILE"
fi

exec "$@"
