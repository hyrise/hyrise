set(
    SHARED_SOURCES
    base_test.cpp
    base_test.hpp
    gtest_case_template.cpp
    gtest_main.cpp
)

set(
    HYRISE_TEST_SOURCES
    ${SHARED_SOURCES}
    concurrency/commit_context_test.cpp
    concurrency/transaction_manager_test.cpp
    lib/AllParameterVariant_test.cpp
    lib/all_type_variant_test.cpp
    operators/aggregate_test.cpp
    operators/delete_test.cpp
    operators/difference_test.cpp
    operators/export_binary_test.cpp
    operators/export_csv_test.cpp
    operators/get_table_test.cpp
    operators/import_binary_test.cpp
    operators/import_csv_test.cpp
    operators/index_column_scan_test.cpp
    operators/insert_test.cpp
    operators/join_equi_test.cpp
    operators/join_full_test.cpp
    operators/join_null_test.cpp
    operators/join_test.hpp
    operators/limit_test.cpp
    operators/print_test.cpp
    operators/product_test.cpp
    operators/projection_test.cpp
    operators/rollback_records_test.cpp
    operators/sort_test.cpp
    operators/table_scan_like_test.cpp
    operators/table_scan_test.cpp
    operators/union_all_test.cpp
    operators/update_test.cpp
    operators/validate_test.cpp
    operators/validate_visibility_test.cpp
    optimizer/abstract_syntax_tree/abstract_syntax_tree_test.cpp
    optimizer/abstract_syntax_tree/aggregate_node_test.cpp
    optimizer/abstract_syntax_tree/join_node_test.cpp
    optimizer/abstract_syntax_tree/stored_table_node_test.cpp
    optimizer/abstract_syntax_tree/projection_node_test.cpp
    optimizer/ast_to_operator_translator_test.cpp
    optimizer/column_statistics_test.cpp
    optimizer/strategy/join_detection_rule_test.cpp
    optimizer/expression_test.cpp
    optimizer/strategy/predicate_reordering_test.cpp
    optimizer/strategy/strategy_base_test.cpp
    optimizer/table_statistics_join_test.cpp
    optimizer/table_statistics_test.cpp
    scheduler/scheduler_test.cpp
    sql/sql_base_test.cpp
    sql/sql_basic_cache_test.cpp
    sql/sql_expression_translator_test.cpp
    sql/sql_parse_tree_cache_test.cpp
    sql/sql_prepare_execute_test.cpp
    sql/sql_query_operator_test.cpp
    sql/sql_query_plan_test.cpp
    sql/sql_query_plan_cache_test.cpp
    sql/sql_to_ast_translator_test.cpp
    sql/sql_to_result_test.cpp
    sql/tpch_test.cpp
    sql/sqlite_testrunner/sqlite_testrunner.cpp
    sql/sqlite_testrunner/sqlite_wrapper.cpp
    sql/sqlite_testrunner/sqlite_wrapper.hpp
    storage/adaptive_radix_tree_index_test.cpp
    storage/chunk_test.cpp
    storage/composite_group_key_index_test.cpp
    storage/dictionary_column_test.cpp
    storage/group_key_index_test.cpp
    storage/iterables_test.cpp
    storage/multi_column_index_test.cpp
    storage/reference_column_test.cpp
    storage/single_column_index_test.cpp
    storage/storage_manager_test.cpp
    storage/table_test.cpp
    storage/value_column_test.cpp
    storage/variable_length_key_base_test.cpp
    storage/variable_length_key_store_test.cpp
    storage/variable_length_key_test.cpp
    tasks/chunk_compression_task_test.cpp
    tasks/operator_task_test.cpp
    utils/cuckoo_hashtable_test.cpp
    utils/numa_memory_resource_test.cpp
)

set (
    TPCC_TEST_SOURCES
    ${SHARED_SOURCES}
    tpc/tpcc_ref_test.cpp
)

# Both hyriseTest and hyriseSanitizers link against these
set(
    LIBRARIES
    gtest
    sqlite3
)

# Build special ASAN version of googletest
include_directories(../../third_party/googletest/googletest/)

set(
    GTEST_SOURCES
    ../../third_party/googletest/googletest/src/gtest-all.cc
)
add_library(gtestAsan EXCLUDE_FROM_ALL STATIC ${GTEST_SOURCES})
set_target_properties(gtestAsan PROPERTIES SUFFIX "_asan")
set_target_properties(gtestAsan PROPERTIES COMPILE_FLAGS "-fsanitize=address -fno-omit-frame-pointer")

set(
    LIBRARIES_ASAN
    gtestAsan
    sqlite3
)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Configure hyriseTest
add_executable(hyriseTest ${HYRISE_TEST_SOURCES})
target_link_libraries(hyriseTest hyrise ${LIBRARIES})

# Configure hyriseCoverageApp
add_executable(hyriseCoverage EXCLUDE_FROM_ALL ${HYRISE_TEST_SOURCES})
target_link_libraries(hyriseCoverage hyriseCoverageLib ${LIBRARIES} --coverage)
set_target_properties(hyriseCoverage PROPERTIES COMPILE_FLAGS "-fprofile-arcs -ftest-coverage")

# Configure hyriseSanitizersApp
add_executable(hyriseSanitizers EXCLUDE_FROM_ALL ${HYRISE_TEST_SOURCES})
target_link_libraries(hyriseSanitizers hyriseSanitizersLib ${LIBRARIES_ASAN} -fsanitize=address)
set_target_properties(hyriseSanitizers PROPERTIES COMPILE_FLAGS "-fsanitize=address -fno-omit-frame-pointer")

# Configure hyriseTestTPCC
add_executable(hyriseTestTPCC ${TPCC_TEST_SOURCES})
target_link_libraries(hyriseTestTPCC hyrise ${LIBRARIES} tpcc)



