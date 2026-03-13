if(NOT TARGET Boost::ut)
    include(FetchContent)

    if(POLICY CMP0175)
        cmake_policy(SET CMP0175 OLD)
    endif()

    FetchContent_Declare(
        boost_ut
        GIT_REPOSITORY https://github.com/boost-ext/ut.git
        GIT_TAG v2.1.0
        GIT_SHALLOW TRUE
    )
    FetchContent_MakeAvailable(boost_ut)
endif()

# dotenv-cpp (header-only, for .env file loading in integration tests)
if(NOT TARGET dotenv)
    include(FetchContent)
    set(BUILD_DOCS OFF CACHE BOOL "Disable dependency docs generation" FORCE)
    FetchContent_Declare(
        laserpants_dotenv
        GIT_REPOSITORY https://github.com/laserpants/dotenv-cpp.git
        GIT_TAG master
        GIT_SHALLOW TRUE
    )
    FetchContent_MakeAvailable(laserpants_dotenv)
endif()

find_package(PkgConfig REQUIRED)

# Add integration test cmake modules path
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

add_executable(tests_integration_ds_mysql
    main.cpp
    mysql/test_query_builder.cpp
)

set_target_properties(tests_integration_ds_mysql PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

target_link_libraries(tests_integration_ds_mysql
    PRIVATE
        ds_mysql
        Boost::ut
        dotenv
)

# Link with MySQL client library
pkg_check_modules(MYSQL REQUIRED mysqlclient)
target_include_directories(tests_integration_ds_mysql PRIVATE ${MYSQL_INCLUDE_DIRS})
target_link_libraries(tests_integration_ds_mysql PRIVATE ${MYSQL_LIBRARIES})

target_include_directories(tests_integration_ds_mysql PRIVATE
    "${CMAKE_SOURCE_DIR}/lib/include"
)

# Setup Docker integration testing fixtures (if not skipped, e.g. in CI)
include(DockerIntegration)
load_env_file()

if(NOT SKIP_DOCKER_MANAGEMENT)
    setup_docker_integration_tests()
else()
    setup_docker_integration_tests_no_docker()
endif()

if(NOT DEFINED ENV_DS_MYSQL_TEST_HOST OR ENV_DS_MYSQL_TEST_HOST STREQUAL "")
    set(ENV_DS_MYSQL_TEST_HOST "127.0.0.1")
endif()
if(NOT DEFINED ENV_DS_MYSQL_TEST_PORT OR ENV_DS_MYSQL_TEST_PORT STREQUAL "")
    set(ENV_DS_MYSQL_TEST_PORT "3307")
endif()
if(NOT DEFINED ENV_DS_MYSQL_TEST_DATABASE OR ENV_DS_MYSQL_TEST_DATABASE STREQUAL "")
    set(ENV_DS_MYSQL_TEST_DATABASE "ds_mysql_test")
endif()
if(NOT DEFINED ENV_DS_MYSQL_TEST_USER OR ENV_DS_MYSQL_TEST_USER STREQUAL "")
    set(ENV_DS_MYSQL_TEST_USER "ds_mysql_test_user")
endif()
if(NOT DEFINED ENV_DS_MYSQL_TEST_PASSWORD OR ENV_DS_MYSQL_TEST_PASSWORD STREQUAL "")
    set(ENV_DS_MYSQL_TEST_PASSWORD "ds_mysql_test_password")
endif()

# Common environment variables (everything except the port when using Docker management,
# since the port is resolved at test runtime by run_tests_wrapper.sh).
set(_INTEGRATION_ENV_COMMON
    "DS_MYSQL_TEST_HOST=${ENV_DS_MYSQL_TEST_HOST}"
    "DS_MYSQL_TEST_DATABASE=${ENV_DS_MYSQL_TEST_DATABASE}"
    "DS_MYSQL_TEST_USER=${ENV_DS_MYSQL_TEST_USER}"
    "DS_MYSQL_TEST_PASSWORD=${ENV_DS_MYSQL_TEST_PASSWORD}"
)

if(NOT SKIP_DOCKER_MANAGEMENT)
    # Local CTest run: docker_up.sh picks an available port at runtime and saves it
    # to build/.mysql_port.  The wrapper script reads that file and injects
    # DS_MYSQL_TEST_PORT into the test process environment before exec'ing
    # the test binary, so the port is always current regardless of what was in
    # the .env file at configure time.
    set(_INTEGRATION_TEST_WRAPPER
        "${CMAKE_SOURCE_DIR}/tests/integration/docker/run_tests_wrapper.sh")
    add_test(NAME tests_integration_ds_mysql
        COMMAND bash "${_INTEGRATION_TEST_WRAPPER}"
                "$<TARGET_FILE:tests_integration_ds_mysql>")
    set_tests_properties(tests_integration_ds_mysql PROPERTIES
        FIXTURES_REQUIRED docker_integration
        ENVIRONMENT "${_INTEGRATION_ENV_COMMON}"
    )
else()
    # CI / externally-provided DB: port is known at configure time via the
    # DS_MYSQL_TEST_PORT environment variable set by the workflow.
    add_test(NAME tests_integration_ds_mysql COMMAND tests_integration_ds_mysql)
    set_tests_properties(tests_integration_ds_mysql PROPERTIES
        FIXTURES_REQUIRED docker_integration
        ENVIRONMENT "${_INTEGRATION_ENV_COMMON};DS_MYSQL_TEST_PORT=${ENV_DS_MYSQL_TEST_PORT}"
    )
endif()
