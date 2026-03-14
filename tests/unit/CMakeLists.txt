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

add_executable(tests_unit_ds_mysql
    main.cpp
    test_ddl.cpp
    test_dml.cpp
    test_dql.cpp
    test_dql_extensions.cpp
    test_field_types.cpp
    test_valid_table.cpp
)

set_target_properties(tests_unit_ds_mysql PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
)

target_link_libraries(tests_unit_ds_mysql
    PRIVATE
        ds_mysql
        Boost::ut
)

target_include_directories(tests_unit_ds_mysql PRIVATE
    "${CMAKE_SOURCE_DIR}/lib/include"
)

add_test(NAME tests_unit_ds_mysql COMMAND tests_unit_ds_mysql)
