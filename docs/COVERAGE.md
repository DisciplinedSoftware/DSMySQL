# Code Coverage

## Prerequisites

```bash
sudo apt-get install gcov lcov
```

## Enable Coverage

### Using CMake Preset (Recommended)

```bash
cmake --preset coverage
cmake --build build-coverage -j$(nproc)
ctest --preset coverage
```

### Manual Configuration

```bash
cmake -DENABLE_COVERAGE=ON -DCMAKE_BUILD_TYPE=Debug -B build-coverage
cmake --build build-coverage -j$(nproc)
ctest --test-dir build-coverage
```

## Generate HTML Report

```bash
./scripts/generate_coverage.sh build-coverage
# Opens: build-coverage/html/index.html
```

## How It Works

1. `ENABLE_COVERAGE=ON` adds `--coverage` compile and link flags
2. Running tests generates `.gcda` coverage data files
3. `lcov` collects the data and `genhtml` produces the HTML report

## Performance Note

Coverage instrumentation adds 10–30% overhead at compile and run time.
Use the separate `build-coverage/` directory to avoid affecting normal builds.
