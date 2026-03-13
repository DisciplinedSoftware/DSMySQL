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

# Disable boost.ut's C++20 module mode — not all compilers/toolchains support
# its module declaration (notably MSVC issues C3378 with /std:c++latest).
add_compile_definitions(BOOST_UT_DISABLE_MODULE)

# Unit tests
add_subdirectory(unit)

# Integration tests (optional - only if Docker is available)
if(BUILD_INTEGRATION_TESTS)
    add_subdirectory(integration)
endif()
