# Shared third-party dependency resolution.

include(FetchContent)

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
