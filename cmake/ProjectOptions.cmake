# Shared project-level build options and defaults.

# Set C++23 standard
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Export compile commands for IDEs
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Set default build type if not specified
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Build options
option(BUILD_EXAMPLES "Build examples" ON)
option(ENABLE_COVERAGE "Enable code coverage" OFF)
option(SKIP_DOCKER_MANAGEMENT "Skip Docker container lifecycle in CTest (use when DB is externally provided, e.g. CI)" OFF)
option(BUILD_INTEGRATION_TESTS "Build integration tests" ON)
