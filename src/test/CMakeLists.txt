set(
    SHARED_SOURCES
    base_test.hpp
    testing_assert.cpp
    testing_assert.hpp
    sql/sqlite_testrunner/sqlite_wrapper.cpp
    sql/sqlite_testrunner/sqlite_wrapper.hpp
)

set(
    HYRISE_UNIT_TEST_SOURCES
    ${SHARED_SOURCES}
    gtest_main.cpp
    logical_query_plan/aggregate_node_test.cpp
    logical_query_plan/join_node_test.cpp
    logical_query_plan/logical_query_plan_test.cpp
    logical_query_plan/projection_node_test.cpp
    logical_query_plan/union_node_test.cpp
    sql/sql_translator_test.cpp
)

set (
    SYSTEM_TEST_SOURCES
    ${SHARED_SOURCES}
    tpc/tpcc_ref_test.cpp
    tpc/tpch_test.cpp
    gtest_main.cpp
)


# Both hyriseTest and hyriseSystemTest link against these
set(
    LIBRARIES
    gtest
    sqlite3
)

# Build special sanitizer version of googletest
include_directories(../../third_party/googletest/googletest/)

set(
    GTEST_SOURCES
    ../../third_party/googletest/googletest/src/gtest-all.cc
)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Configure hyriseTest
add_executable(hyriseTest ${HYRISE_UNIT_TEST_SOURCES})
target_link_libraries(hyriseTest hyrise ${LIBRARIES})

# Configure hyriseSystemTest
add_executable(hyriseSystemTest ${SYSTEM_TEST_SOURCES})
target_link_libraries(hyriseSystemTest hyrise hyriseBenchmarkLib ${LIBRARIES})
add_dependencies(hyriseSystemTest tpccTableGenerator)
