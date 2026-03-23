# DSMySQL Quick Reference

## Build & Test

```bash
# Configure and build (release)
cmake --preset release
cmake --build build/release -j$(nproc)

# Run all tests
ctest --preset release

# Unit tests only (no database required)
ctest --preset release -R tests_unit_ds_mysql

# Integration tests only (requires Docker)
ctest --preset release -R tests_integration_ds_mysql

# Debug build
cmake --preset debug
cmake --build build/debug -j$(nproc)

# Coverage build
cmake --preset coverage
cmake --build build/coverage -j$(nproc)
ctest --preset coverage
```

## Scripts Layout

- `scripts/ci/act-ci.sh` — run `tests-gcc` job locally with `act`
- `scripts/ci/act-ci-full.sh` — run Linux CI jobs locally in sequence with `act`
- `scripts/release/release.sh` — bump version, update changelog, commit, and tag
- `scripts/release/act-release.sh` — dry-run release workflow locally with `act`

## Project Layout

```text
DSMySQL/
├── lib/include/ds_mysql/   # Header-only library
├── tests/unit/             # Unit tests (boost.ut)
├── tests/integration/      # Integration tests (MySQL + Docker)
├── examples/               # Usage examples
└── docs/                   # Documentation
```

## Key Headers

| Header | Purpose |
|--------|---------|
| `ds_mysql.hpp` | **Umbrella header** — include this to get everything |
| `ds_mysql/database.hpp` | MySQL connection & queries |
| `ds_mysql/sql.hpp` | SQL query builder |
| `ds_mysql/sql_extension.hpp` | MySQL-specific extensions |
| `ds_mysql/sql_temporal.hpp` | Temporal types: `datetime_type`, `timestamp_type`, `time_type` |
| `ds_mysql/metadata.hpp` | MySQL information_schema types |
| `ds_mysql/column_field.hpp` | Typed column descriptors |
| `ds_mysql/schema_generator.hpp` | Schema generation |
| `ds_mysql/config.hpp` | Connection configuration |
| `ds_mysql/sql_varchar.hpp` | Fixed-length string type |

## Environment Variables (Integration Tests)

| Variable | Default |
|----------|---------|
| `DS_MYSQL_TEST_HOST` | `127.0.0.1` |
| `DS_MYSQL_TEST_PORT` | `3307` |
| `DS_MYSQL_TEST_DATABASE` | `ds_mysql_test` |
| `DS_MYSQL_TEST_USER` | `ds_mysql_test_user` |
| `DS_MYSQL_TEST_PASSWORD` | `ds_mysql_test_password` |

## Documentation Index

- `README.md` — Overview and quick start
- `docs/BUILD.md` — Build instructions
- `docs/DEVELOPMENT.md` — Development guide
- `docs/INTEGRATION_TESTS.md` — Integration testing setup
- `docs/COVERAGE.md` — Code coverage setup
- `docs/QUICKREF.md` — This file
