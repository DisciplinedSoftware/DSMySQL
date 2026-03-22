# DSMySQL Build Guide

## Requirements

- CMake 3.25+
- C++23 compiler (GCC 13+ or Clang 16+)
- Git
- MySQL client library (`libmysqlclient-dev`)
- Ninja build system

## Quick Build

```bash
cmake --preset release
cmake --build build/release -j$(nproc)
ctest --preset release
```

## Source Layout

```
DSMySQL/
├── lib/include/ds_mysql/   # All public headers (header-only library)
├── tests/unit/             # Unit tests
├── tests/integration/      # MySQL integration tests
├── examples/               # Usage examples
└── docs/                   # Documentation
```

## Configure + Build Manually

```bash
cmake -S . -B build/release -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=g++-15
cmake --build build/release -j$(nproc)
```

## Build Options

| Option | Default | Description |
|--------|---------|-------------|
| `BUILD_TESTING` | ON | Build unit and integration tests |
| `BUILD_INTEGRATION_TESTS` | ON | Build integration tests (requires MySQL) |
| `BUILD_EXAMPLES` | ON | Build example programs |
| `ENABLE_COVERAGE` | OFF | Enable code coverage instrumentation |
| `SKIP_DOCKER_MANAGEMENT` | OFF | Skip Docker lifecycle in CTest (use in CI) |

## Run Tests

```bash
# Unit tests only (no database required)
ctest --preset release -R tests_unit_ds_mysql

# All tests (requires Docker for integration tests)
ctest --preset release

# Skip integration tests
cmake --preset release -DBUILD_INTEGRATION_TESTS=OFF
cmake --build build/release -j$(nproc)
ctest --preset release
```

## Code Coverage

```bash
cmake --preset coverage
cmake --build build/coverage -j$(nproc)
ctest --preset coverage
./scripts/generate_coverage.sh build/coverage
# Report: build/coverage/html/index.html
```
