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
    base_test.hpp
    concurrency/commit_context_test.cpp
    concurrency/transaction_context_test.cpp
    gtest_main.cpp
    import_export/csv_meta_test.cpp
    lib/all_parameter_variant_test.cpp
    lib/all_type_variant_test.cpp
    logical_query_plan/aggregate_node_test.cpp
    logical_query_plan/create_view_node_test.cpp
    logical_query_plan/drop_view_node_test.cpp
    logical_query_plan/lqp_find_first_subplan_mismatch_test.cpp
    logical_query_plan/delete_node_test.cpp
    logical_query_plan/dummy_table_node_test.cpp
    logical_query_plan/insert_node_test.cpp
    logical_query_plan/join_node_test.cpp
    logical_query_plan/limit_node_test.cpp
    logical_query_plan/logical_query_plan_test.cpp
    logical_query_plan/mock_node_test.cpp
    logical_query_plan/predicate_node_test.cpp
    logical_query_plan/projection_node_test.cpp
    logical_query_plan/show_columns_node_test.cpp
    logical_query_plan/show_tables_node_test.cpp
    logical_query_plan/sort_node_test.cpp
    logical_query_plan/stored_table_node_test.cpp
    logical_query_plan/union_node_test.cpp
    logical_query_plan/update_node_test.cpp
    logical_query_plan/validate_node_test.cpp
    operators/aggregate_test.cpp
    operators/delete_test.cpp
    operators/difference_test.cpp
    operators/export_binary_test.cpp
    operators/export_csv_test.cpp
    operators/get_table_test.cpp
    operators/import_binary_test.cpp
    operators/import_csv_test.cpp
    operators/index_scan_test.cpp
    operators/insert_test.cpp
    operators/join_equi_test.cpp
    operators/join_full_test.cpp
    operators/join_hash_test.cpp
    operators/join_index_test.cpp
    operators/join_null_test.cpp
    operators/join_semi_anti_test.cpp
    operators/join_test.hpp
    operators/limit_test.cpp
    operators/physical_query_plan_test.cpp
    operators/maintenance/create_view_test.cpp
    operators/maintenance/drop_view_test.cpp
    operators/maintenance/show_columns_test.cpp
    operators/maintenance/show_tables_test.cpp
    operators/print_test.cpp
    operators/product_test.cpp
    operators/projection_test.cpp
    operators/recreation_test.cpp
    operators/sort_test.cpp
    operators/table_scan_like_test.cpp
    operators/table_scan_test.cpp
    operators/union_all_test.cpp
    operators/union_positions_test.cpp
    operators/update_test.cpp
    operators/validate_test.cpp
    operators/validate_visibility_test.cpp
    optimizer/expression_test.cpp
    optimizer/join_ordering/join_graph_builder_test.cpp
    optimizer/join_ordering/join_graph_test.cpp
    optimizer/join_ordering/join_plan_predicate_test.cpp
    optimizer/lqp_translator_test.cpp
    optimizer/optimizer_test.cpp
    optimizer/strategy/chunk_pruning_test.cpp
    optimizer/strategy/constant_calculation_rule_test.cpp
    optimizer/strategy/index_scan_rule_test.cpp
    optimizer/strategy/join_detection_rule_test.cpp
    optimizer/strategy/predicate_reordering_test.cpp
    optimizer/strategy/predicate_pushdown_rule_test.cpp
    optimizer/strategy/strategy_base_test.cpp
    optimizer/strategy/strategy_base_test.hpp
    statistics/table_statistics_join_test.cpp
    statistics/table_statistics_test.cpp
    statistics/chunk_statistics/pruning_filters_test.cpp
    statistics/column_statistics_test.cpp
    statistics/generate_table_statistics_test.cpp
    statistics/statistics_import_export_test.cpp
    statistics/statistics_test_utils.hpp
    scheduler/scheduler_test.cpp
    server/mock_connection.hpp
    server/mock_task_runner.hpp
    server/postgres_wire_handler_test.cpp
    server/server_session_test.cpp
    sql/sql_basic_cache_test.cpp
    sql/hsql_expression_translator_test.cpp
    sql/sqlite_testrunner/sqlite_testrunner.cpp
    sql/sqlite_testrunner/sqlite_wrapper.cpp
    sql/sqlite_testrunner/sqlite_wrapper.hpp
    sql/sql_pipeline_statement_test.cpp
    sql/sql_pipeline_test.cpp
    sql/sql_query_plan_cache_test.cpp
    sql/sql_query_plan_test.cpp
    sql/sql_translator_test.cpp
    storage/adaptive_radix_tree_index_test.cpp
    storage/any_column_iterable_test.cpp
    storage/chunk_encoder_test.cpp
    storage/chunk_test.cpp
    storage/composite_group_key_index_test.cpp
    storage/dictionary_column_test.cpp
    storage/encoding_test.hpp
    storage/encoded_column_test.cpp
    storage/group_key_index_test.cpp
    storage/iterables_test.cpp
    storage/materialize_test.cpp
    storage/multi_column_index_test.cpp
    storage/compressed_vector_test.cpp
    storage/numa_placement_test.cpp
    storage/reference_column_test.cpp
    storage/simd_bp128_test.cpp
    storage/single_column_index_test.cpp
    storage/storage_manager_test.cpp
    storage/table_test.cpp
    storage/value_column_test.cpp
    storage/variable_length_key_base_test.cpp
    storage/variable_length_key_store_test.cpp
    storage/variable_length_key_test.cpp
    tasks/chunk_compression_task_test.cpp
    tasks/operator_task_test.cpp
    testing_assert.cpp
    testing_assert.hpp
    utils/cuckoo_hashtable_test.cpp
    utils/format_bytes_test.cpp
    utils/format_duration_test.cpp
    utils/numa_memory_resource_test.cpp
    gtest_main.cpp
)

if (${ENABLE_JIT_SUPPORT})
    set(HYRISE_UNIT_TEST_SOURCES
        ${HYRISE_UNIT_TEST_SOURCES}
        operators/jit_operator_wrapper_test.cpp
        operators/jit_operator/jit_aware_lqp_translator_test.cpp
        operators/jit_operator/jit_operations_test.cpp
        operators/jit_operator/jit_tuple_value_test.cpp
        operators/jit_operator/jit_variant_vector_test.cpp
        operators/jit_operator/operators/jit_compute_test.cpp
        operators/jit_operator/operators/jit_expression_test.cpp
        operators/jit_operator/operators/jit_filter_test.cpp
        operators/jit_operator/operators/jit_read_write_tuple_test.cpp
        operators/jit_operator/specialization/jit_compiler_test.cpp
        operators/jit_operator/specialization/jit_repository_test.cpp
    )
endif()

set (
    SYSTEM_TEST_SOURCES
    ${SHARED_SOURCES}
    server/server_test_runner.cpp
    tpc/tpch_test.cpp
    tpc/tpch_db_generator_test.cpp
    gtest_main.cpp
)


# Both hyriseTest and hyriseSystemTest link against these
set(
    LIBRARIES
    gtest
    gmock
    sqlite3
    ${FILESYSTEM_LIBRARY}
    ${Boost_SYSTEM_LIBRARY}
    ${Boost_THREAD_LIBRARY}
)

# Build special sanitizer version of googletest
include_directories(../../third_party/googletest/googletest/)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Configure hyriseTest
add_executable(hyriseTest ${HYRISE_UNIT_TEST_SOURCES})
target_link_libraries(hyriseTest hyrise ${LIBRARIES})

# Configure hyriseSystemTest
add_executable(hyriseSystemTest ${SYSTEM_TEST_SOURCES})
target_link_libraries(hyriseSystemTest hyrise hyriseBenchmarkLib ${LIBRARIES} pqxx)
