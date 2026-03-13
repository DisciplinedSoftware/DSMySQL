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

# Unit tests
add_subdirectory(unit)

# Integration tests (optional - only if Docker is available)
if(BUILD_INTEGRATION_TESTS)
    add_subdirectory(integration)
endif()
