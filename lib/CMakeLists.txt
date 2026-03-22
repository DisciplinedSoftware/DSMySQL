# DSMySQL is a header-only interface library.
# No compiled sources — all functionality is in lib/include/ds_mysql/.

# Generate version.hpp from the template, placing it in the build tree so that
# CMakeLists.txt project(VERSION ...) remains the single source of truth.
configure_file(
    include/ds_mysql/version.hpp.in
    include/ds_mysql/version_generated.hpp
    @ONLY
)

add_library(ds_mysql INTERFACE)
add_library(ds_mysql::ds_mysql ALIAS ds_mysql)

option(DSMYSQL_ENABLE_INSTALL "Enable install targets" ${PROJECT_IS_TOP_LEVEL})

target_include_directories(ds_mysql
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    "$<BUILD_INTERFACE:${MYSQL_INCLUDE_DIRS}>"
)

target_link_libraries(ds_mysql
    INTERFACE
        $<BUILD_INTERFACE:Boost::pfr>
    "$<BUILD_INTERFACE:${MYSQL_LIBRARIES}>"
)

if(DSMYSQL_ENABLE_INSTALL AND NOT CMAKE_SKIP_INSTALL_RULES)
    include(CMakePackageConfigHelpers)
    include(GNUInstallDirs)

    set(DSMYSQL_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/ds_mysql")

    install(
        TARGETS ds_mysql
        EXPORT ds_mysqlTargets
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )

    install(
        DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/ds_mysql/
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/ds_mysql
        FILES_MATCHING
        PATTERN "*.hpp"
    )

    install(
        FILES ${CMAKE_CURRENT_BINARY_DIR}/include/ds_mysql/version_generated.hpp
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/ds_mysql
    )

    write_basic_package_version_file(
        ${CMAKE_CURRENT_BINARY_DIR}/ds_mysqlConfigVersion.cmake
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion
    )

    configure_package_config_file(
        ${PROJECT_SOURCE_DIR}/cmake/ds_mysqlConfig.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/ds_mysqlConfig.cmake
        INSTALL_DESTINATION ${DSMYSQL_INSTALL_CMAKEDIR}
    )

    install(
        EXPORT ds_mysqlTargets
        FILE ds_mysqlTargets.cmake
        NAMESPACE ds_mysql::
        DESTINATION ${DSMYSQL_INSTALL_CMAKEDIR}
    )

    install(
        FILES
            ${CMAKE_CURRENT_BINARY_DIR}/ds_mysqlConfig.cmake
            ${CMAKE_CURRENT_BINARY_DIR}/ds_mysqlConfigVersion.cmake
        DESTINATION ${DSMYSQL_INSTALL_CMAKEDIR}
    )
endif()
