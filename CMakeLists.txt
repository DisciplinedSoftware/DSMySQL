cmake_minimum_required(VERSION 3.25)

project(DSMySQL
    VERSION 3.0.0
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

# Make local CMake helper modules available early (needed by fetched deps).
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(ProjectOptions)
include(CompilerOptions)
include(Dependencies)

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
