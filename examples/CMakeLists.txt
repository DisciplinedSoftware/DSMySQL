if(NOT TARGET Boost::ut)
    include(FetchContent)
endif()

# All examples link against ds_mysql (header-only) and the MySQL client
add_executable(example_basic_query basic_query.cpp)
target_link_libraries(example_basic_query PRIVATE ds_mysql)
set_target_properties(example_basic_query PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin/examples"
)

add_executable(example_schema_generation schema_generation.cpp)
target_link_libraries(example_schema_generation PRIVATE ds_mysql)
set_target_properties(example_schema_generation PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin/examples"
)

add_executable(example_tables_with_constraints tables_with_constraints.cpp)
target_link_libraries(example_tables_with_constraints PRIVATE ds_mysql)
set_target_properties(example_tables_with_constraints PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin/examples"
)
