# Shared compiler warnings and instrumentation flags.

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
