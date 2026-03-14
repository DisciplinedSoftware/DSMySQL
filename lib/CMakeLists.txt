# DSMySQL is a header-only interface library.
# No compiled sources — all functionality is in lib/include/ds_mysql/.

# Generate version.hpp from the template, placing it in the build tree so that
# CMakeLists.txt project(VERSION ...) remains the single source of truth.
configure_file(
    include/ds_mysql/version.hpp.in
    include/ds_mysql/version.hpp
    @ONLY
)

add_library(ds_mysql INTERFACE)

target_include_directories(ds_mysql
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/include>
        $<INSTALL_INTERFACE:include>
        ${MYSQL_INCLUDE_DIRS}
)

target_link_libraries(ds_mysql
    INTERFACE
        Boost::pfr
        ${MYSQL_LIBRARIES}
)
