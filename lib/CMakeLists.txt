# DSMySQL is a header-only interface library.
# No compiled sources — all functionality is in lib/include/ds_mysql/.

add_library(ds_mysql INTERFACE)

target_include_directories(ds_mysql
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
        ${MYSQL_INCLUDE_DIRS}
)

target_link_libraries(ds_mysql
    INTERFACE
        Boost::pfr
        ${MYSQL_LIBRARIES}
)
