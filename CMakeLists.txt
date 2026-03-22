cmake_minimum_required(VERSION 3.25)

project(DSMySQL
    VERSION 1.0.0
    DESCRIPTION "A portable C++23 type-safe MySQL query builder and database wrapper"
    LANGUAGES CXX
)

# Configure git hooks at cmake configure time
if(EXISTS "${CMAKE_SOURCE_DIR}/.githooks")
    execute_process(
        COMMAND git config core.hooksPath .githooks
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    )
endif()

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

# Compiler options
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(
        -Wall
        -Wextra
        -Wpedantic
        -Werror
    )
elseif(MSVC)
    add_compile_options(
        /W4       # High warning level
        /WX       # Warnings as errors
        /permissive-  # Strict standards conformance
        /Zc:preprocessor # Enable standards-conforming preprocessor (__VA_OPT__)
    )
endif()

# Coverage flags
if(ENABLE_COVERAGE)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        add_compile_options(--coverage)
        add_link_options(--coverage)
        message(STATUS "Coverage enabled with --coverage flags")
    else()
        message(WARNING "Coverage is not supported with ${CMAKE_CXX_COMPILER_ID}")
    endif()
endif()

# Include FetchContent for dependency management
include(FetchContent)

# Make local CMake helper modules available early (needed by fetched deps).
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# MySQL client library
if(WIN32)
    # On Windows, search common MySQL Server installation paths.
    # Override by passing -DMYSQL_ROOT_DIR=<path> to cmake.
    set(MYSQL_ROOT_DIR "" CACHE PATH "MySQL installation root (e.g. C:/Program Files/MySQL/MySQL Server 8.0)")
    if(NOT MYSQL_ROOT_DIR)
        file(GLOB _mysql_dirs "C:/Program Files/MySQL/MySQL Server *")
        if(_mysql_dirs)
            list(SORT _mysql_dirs ORDER DESCENDING)
            list(GET _mysql_dirs 0 MYSQL_ROOT_DIR)
            set(MYSQL_ROOT_DIR "${MYSQL_ROOT_DIR}" CACHE PATH "" FORCE)
        else()
            message(FATAL_ERROR "MySQL not found. Set -DMYSQL_ROOT_DIR=<path> to the MySQL installation directory.")
        endif()
    endif()

    find_path(MYSQL_INCLUDE_DIR mysql.h PATHS "${MYSQL_ROOT_DIR}/include" NO_DEFAULT_PATH REQUIRED)
    find_library(MYSQL_LIBRARY libmysql PATHS "${MYSQL_ROOT_DIR}/lib" NO_DEFAULT_PATH REQUIRED)
    set(MYSQL_INCLUDE_DIRS "${MYSQL_INCLUDE_DIR}")
    set(MYSQL_LIBRARIES "${MYSQL_LIBRARY}")
    message(STATUS "MySQL found at: ${MYSQL_ROOT_DIR}")
else()
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(MYSQL REQUIRED mysqlclient)
endif()

# Note: Boost.PFR is header-only and doesn't have a standard CMake config package yet,
# so we fetch it directly without find_package. Once Boost.PFR is officially integrated
# into Boost's CMake build system, we can use: find_package(Boost REQUIRED COMPONENTS pfr CONFIG)

# Boost.PFR (header-only, for compile-time reflection without macros)
# We only need the headers; FetchContent_Populate avoids processing Boost.PFR's own
# CMake build system, whose tests require Boost::core which is unavailable here.
FetchContent_Declare(
    boost_pfr_headers
    GIT_REPOSITORY https://github.com/boostorg/pfr.git
    GIT_TAG master
    GIT_SHALLOW TRUE
)
# CMP0169 OLD: suppress the single-arg FetchContent_Populate deprecation warning.
# This is intentional — we want headers only, not add_subdirectory behaviour.
cmake_policy(PUSH)
if(POLICY CMP0169)
    cmake_policy(SET CMP0169 OLD)
endif()
FetchContent_Populate(boost_pfr_headers)
cmake_policy(POP)

# Create Boost.PFR target if not already provided by Boost.PFR's own CMake
if(NOT TARGET Boost::pfr)
    add_library(Boost::pfr INTERFACE IMPORTED)
    set_target_properties(Boost::pfr PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${boost_pfr_headers_SOURCE_DIR}/include"
        INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${boost_pfr_headers_SOURCE_DIR}/include"
    )
endif()

# Auto-create .env from .env.example in tests/integration if missing
if(NOT EXISTS "${CMAKE_SOURCE_DIR}/tests/integration/.env")
    if(EXISTS "${CMAKE_SOURCE_DIR}/tests/integration/.env.example")
        configure_file(
            "${CMAKE_SOURCE_DIR}/tests/integration/.env.example"
            "${CMAKE_SOURCE_DIR}/tests/integration/.env"
            COPYONLY
        )
        message(STATUS "✓ Created tests/integration/.env from .env.example")
    endif()
endif()

# Install git hooks on configure
if(EXISTS "${CMAKE_SOURCE_DIR}/.githooks")
    file(COPY ${CMAKE_SOURCE_DIR}/.githooks/
         DESTINATION ${CMAKE_SOURCE_DIR}/.git/hooks
         FILE_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE)
    message(STATUS "✓ Git hooks installed")
endif()

# Enable testing via CTest (defines BUILD_TESTING)
include(CTest)

# Setup code coverage if enabled
if(ENABLE_COVERAGE)
    include(CodeCoverage)
endif()

# Add library
add_subdirectory(lib)

# Add tests and examples only when DSMySQL is the top-level project
if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    # Add tests
    if(BUILD_TESTING)
        add_subdirectory(tests)
    endif()

    # Add examples
    if(BUILD_EXAMPLES)
        add_subdirectory(examples)
    endif()
endif()

# Display configuration summary
message(STATUS "")
message(STATUS "===== DSMySQL Configuration =====")
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ compiler: ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "C++ standard: C++${CMAKE_CXX_STANDARD}")
message(STATUS "Build tests: ${BUILD_TESTING}")
message(STATUS "Build integration tests: ${BUILD_INTEGRATION_TESTS}")
message(STATUS "Skip Docker management: ${SKIP_DOCKER_MANAGEMENT}")
message(STATUS "Build examples: ${BUILD_EXAMPLES}")
message(STATUS "Enable coverage: ${ENABLE_COVERAGE}")
message(STATUS "Output directory: ${CMAKE_BINARY_DIR}")
message(STATUS "================================")
message(STATUS "")
