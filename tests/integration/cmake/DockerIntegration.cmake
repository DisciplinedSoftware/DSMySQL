# Docker container management helper for integration tests
# This module provides functions to manage Docker containers for testing

# Function to read .env file and set environment variables
function(load_env_file)
    set(ENV_FILE "${CMAKE_SOURCE_DIR}/tests/integration/.env")
    if(NOT EXISTS "${ENV_FILE}")
        message(FATAL_ERROR ".env file not found at ${ENV_FILE}. Run: cp tests/integration/.env.example tests/integration/.env")
    endif()

    file(STRINGS "${ENV_FILE}" ENV_FILE_CONTENTS)
    foreach(line ${ENV_FILE_CONTENTS})
        if(NOT line MATCHES "^#" AND NOT line STREQUAL "")
            if(line MATCHES "^[A-Z_]+=")
                string(REGEX MATCH "^([A-Z_]+)=(.*)" _ "${line}")
                set(ENV_VAR_NAME "${CMAKE_MATCH_1}")
                set(ENV_VAR_VALUE "${CMAKE_MATCH_2}")
                set(ENV_${ENV_VAR_NAME} "${ENV_VAR_VALUE}" PARENT_SCOPE)
            endif()
        endif()
    endforeach()

    # System environment variables override .env file values (e.g. in CI)
    foreach(VAR DS_MYSQL_TEST_HOST DS_MYSQL_TEST_PORT DS_MYSQL_TEST_DATABASE
                DS_MYSQL_TEST_USER DS_MYSQL_TEST_PASSWORD)
        if(DEFINED ENV{${VAR}})
            set(ENV_${VAR} "$ENV{${VAR}}" PARENT_SCOPE)
        endif()
    endforeach()
endfunction()

# Function to setup Docker container lifecycle for integration tests
function(setup_docker_integration_tests)
    set(DOCKER_COMPOSE_FILE "${CMAKE_SOURCE_DIR}/tests/integration/docker/docker-compose.yml")
    set(DOCKER_UP_SCRIPT "${CMAKE_SOURCE_DIR}/tests/integration/docker/docker_up.sh")
    set(DOCKER_DOWN_SCRIPT "${CMAKE_SOURCE_DIR}/tests/integration/docker/docker_down.sh")
    set(DOCKER_WRAPPER_SCRIPT "${CMAKE_SOURCE_DIR}/tests/integration/docker/run_tests_wrapper.sh")

    # Load environment variables from .env
    load_env_file()

    # Make scripts executable
    file(CHMOD "${DOCKER_UP_SCRIPT}" FILE_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE)
    file(CHMOD "${DOCKER_DOWN_SCRIPT}" FILE_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE)
    file(CHMOD "${DOCKER_WRAPPER_SCRIPT}" FILE_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE)

    # Add CTest fixtures for docker container lifecycle
    # Labelled "fixture" so VS Code test explorer excludes them from standalone runs
    # (via cmake.ctestArgs --label-exclude fixture). CTest still runs them as
    # fixture dependencies when tests_integration_ds_mysql is selected.
    add_test(NAME docker_integration_setup COMMAND bash "${DOCKER_UP_SCRIPT}")
    set_tests_properties(docker_integration_setup PROPERTIES
        FIXTURES_SETUP docker_integration
        EXCLUDE_FROM_DEFAULT_RUN TRUE
    )

    add_test(NAME docker_integration_teardown COMMAND bash "${DOCKER_DOWN_SCRIPT}")
    set_tests_properties(docker_integration_teardown PROPERTIES
        FIXTURES_CLEANUP docker_integration
        EXCLUDE_FROM_DEFAULT_RUN TRUE
    )

    # Custom targets for manual container management
    add_custom_target(docker_integration_up
        COMMENT "Starting MySQL container for integration tests"
        COMMAND bash "${DOCKER_UP_SCRIPT}"
    )

    add_custom_target(docker_integration_down
        COMMENT "Stopping MySQL container for integration tests"
        COMMAND bash "${DOCKER_DOWN_SCRIPT}"
    )

endfunction()


# Variant used in CI where the database is provided externally (e.g. GitHub Actions service container).
# Skips docker_up/docker_down fixtures while keeping the fixture label so integration tests still run.
function(setup_docker_integration_tests_no_docker)
    # Register no-op setup/teardown so FIXTURES_REQUIRED on integration tests still resolves
    add_test(NAME docker_integration_setup COMMAND ${CMAKE_COMMAND} -E true)
    set_tests_properties(docker_integration_setup PROPERTIES
        FIXTURES_SETUP docker_integration
        EXCLUDE_FROM_DEFAULT_RUN TRUE
    )

    add_test(NAME docker_integration_teardown COMMAND ${CMAKE_COMMAND} -E true)
    set_tests_properties(docker_integration_teardown PROPERTIES
        FIXTURES_CLEANUP docker_integration
        EXCLUDE_FROM_DEFAULT_RUN TRUE
    )
endfunction()
