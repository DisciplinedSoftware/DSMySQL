# Integration Testing with Docker

DSMySQL uses Docker containers to provide a real MySQL 8.0 instance for integration testing.

## Overview

- **Unit Tests** (`tests/unit/`): In-memory tests, no external dependencies, run fast
- **Integration Tests** (`tests/integration/mysql/`): Require a running MySQL 8.0 database

## Quick Start

```bash
cmake --preset release
cmake --build build -j$(nproc)
ctest --preset release
```

CTest automatically:

1. Starts a MySQL container via Docker Compose
2. Runs all unit and integration tests
3. Stops the MySQL container

## Configuration

Integration tests read connection parameters from environment variables or `tests/integration/.env`.

| Variable | Default | Description |
|----------|---------|-------------|
| `DS_MYSQL_TEST_HOST` | `127.0.0.1` | MySQL server hostname |
| `DS_MYSQL_TEST_PORT` | `3307` | MySQL server port |
| `DS_MYSQL_TEST_DATABASE` | `ds_mysql_test` | Database name |
| `DS_MYSQL_TEST_USER` | `ds_mysql_test_user` | Username |
| `DS_MYSQL_TEST_PASSWORD` | `ds_mysql_test_password` | Password |

The `.env` file is auto-created from `.env.example` during CMake configure if missing.

## Manual Container Management

```bash
# Start container only
cmake --build build --target docker_integration_up

# Stop container
cmake --build build --target docker_integration_down
```

## CI / Externally-Provided Database

Set `SKIP_DOCKER_MANAGEMENT=ON` to disable Docker fixture management when a MySQL
instance is provided externally (e.g. GitHub Actions service container):

```bash
cmake --preset release -DSKIP_DOCKER_MANAGEMENT=ON
```

## Container Details

- **Image**: MySQL 8.0
- **Container Name**: `ds_mysql_test`
- **Default Port**: 3307 (auto-selects an available port if occupied)

## Troubleshooting

### `.env` file not found

```bash
cp tests/integration/.env.example tests/integration/.env
```

### Port 3307 already in use

The `docker_up.sh` script automatically finds an available port starting from 3307.
You can also override it in `.env`:

```
DS_MYSQL_TEST_PORT=3308
```

### Container not starting

```bash
docker logs ds_mysql_test
docker stop ds_mysql_test && docker rm ds_mysql_test
cmake --build build --target docker_integration_up
```
