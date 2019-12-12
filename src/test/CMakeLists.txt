set(
    SHARED_SOURCES
    base_test.hpp
    testing_assert.cpp
    testing_assert.hpp
    sql/sqlite_testrunner/sqlite_testrunner.cpp
    sql/sqlite_testrunner/sqlite_testrunner.hpp
)

set(
    HYRISE_UNIT_TEST_SOURCES
    ${SHARED_SOURCES}
    benchmarklib/sqlite_add_indices_test.cpp
    benchmarklib/table_builder_test.cpp
    cache/cache_test.cpp
    concurrency/commit_context_test.cpp
    concurrency/transaction_context_test.cpp
    concurrency/transaction_manager_test.cpp
    cost_estimation/abstract_cost_estimator_test.cpp
    expression/expression_evaluator_to_pos_list_test.cpp
    expression/expression_evaluator_to_values_test.cpp
    expression/expression_result_test.cpp
    expression/expression_test.cpp
    expression/expression_utils_test.cpp
    expression/like_matcher_test.cpp
    expression/lqp_subquery_expression_test.cpp
    expression/pqp_subquery_expression_test.cpp
    gtest_case_template.cpp
    gtest_main.cpp
    import_export/csv_meta_test.cpp
    lib/all_parameter_variant_test.cpp
    lib/all_type_variant_test.cpp
    lib/hyrise_test.cpp
    lib/import_export/csv_parser_test.cpp
    lib/fixed_string_test.cpp
    lib/null_value_test.cpp
    lib/utils/load_table_test.cpp
    lib/utils/verify_tables_test.cpp
    logical_query_plan/aggregate_node_test.cpp
    logical_query_plan/alias_node_test.cpp
    logical_query_plan/create_view_node_test.cpp
    logical_query_plan/create_prepared_plan_node_test.cpp
    logical_query_plan/create_table_node_test.cpp
    logical_query_plan/delete_node_test.cpp
    logical_query_plan/drop_table_node_test.cpp
    logical_query_plan/drop_view_node_test.cpp
    logical_query_plan/dummy_table_node_test.cpp
    logical_query_plan/insert_node_test.cpp
    logical_query_plan/join_node_test.cpp
    logical_query_plan/limit_node_test.cpp
    logical_query_plan/logical_query_plan_test.cpp
    logical_query_plan/lqp_find_subplan_mismatch_test.cpp
    logical_query_plan/lqp_utils_test.cpp
    logical_query_plan/lqp_translator_test.cpp
    logical_query_plan/mock_node_test.cpp
    logical_query_plan/predicate_node_test.cpp
    logical_query_plan/projection_node_test.cpp
    logical_query_plan/sort_node_test.cpp
    logical_query_plan/static_table_node_test.cpp
    logical_query_plan/stored_table_node_test.cpp
    logical_query_plan/union_node_test.cpp
    logical_query_plan/update_node_test.cpp
    logical_query_plan/validate_node_test.cpp
    lossless_cast_test.cpp
    memory/numa_memory_resource_test.cpp
    operators/aggregate_test.cpp
    operators/alias_operator_test.cpp
    operators/delete_test.cpp
    operators/difference_test.cpp
    operators/export_binary_test.cpp
    operators/export_csv_test.cpp
    operators/get_table_test.cpp
    operators/import_binary_test.cpp
    operators/import_csv_test.cpp
    operators/index_scan_test.cpp
    operators/insert_test.cpp
    operators/join_hash_test.cpp
    operators/join_hash_types_test.cpp
    operators/join_hash_steps_test.cpp
    operators/join_hash_traits_test.cpp
    operators/join_index_test.cpp
    operators/join_nested_loop_test.cpp
    operators/join_sort_merge_test.cpp
    operators/join_test_runner.cpp
    operators/join_verification_test.cpp
    operators/limit_test.cpp
    operators/maintenance/create_view_test.cpp
    operators/maintenance/create_prepared_plan_test.cpp
    operators/maintenance/create_table_test.cpp
    operators/maintenance/drop_view_test.cpp
    operators/maintenance/drop_table_test.cpp
    operators/operator_deep_copy_test.cpp
    operators/operator_join_predicate_test.cpp
    operators/operator_scan_predicate_test.cpp
    operators/print_test.cpp
    operators/product_test.cpp
    operators/projection_test.cpp
    operators/sort_test.cpp
    operators/table_scan_between_test.cpp
    operators/table_scan_sorted_segment_search_test.cpp
    operators/table_scan_string_test.cpp
    operators/table_scan_test.cpp
    operators/typed_operator_base_test.hpp
    operators/union_all_test.cpp
    operators/union_positions_test.cpp
    operators/update_test.cpp
    operators/validate_test.cpp
    operators/validate_visibility_test.cpp
    optimizer/dp_ccp_test.cpp
    optimizer/greedy_operator_ordering_test.cpp
    optimizer/enumerate_ccp_test.cpp
    optimizer/join_graph_builder_test.cpp
    optimizer/join_graph_test.cpp
    optimizer/optimizer_test.cpp
    optimizer/strategy/between_composition_rule_test.cpp
    optimizer/strategy/chunk_pruning_rule_test.cpp
    optimizer/strategy/column_pruning_rule_test.cpp
    optimizer/strategy/expression_reduction_rule_test.cpp
    optimizer/strategy/index_scan_rule_test.cpp
    optimizer/strategy/in_expression_rewrite_rule_test.cpp
    optimizer/strategy/join_ordering_rule_test.cpp
    optimizer/strategy/join_predicate_ordering_rule_test.cpp
    optimizer/strategy/predicate_merge_rule_test.cpp
    optimizer/strategy/predicate_placement_rule_test.cpp
    optimizer/strategy/predicate_reordering_rule_test.cpp
    optimizer/strategy/predicate_split_up_rule_test.cpp
    optimizer/strategy/semi_join_reduction_rule_test.cpp
    optimizer/strategy/strategy_base_test.cpp
    optimizer/strategy/strategy_base_test.hpp
    optimizer/strategy/subquery_to_join_rule_test.cpp
    plugins/mvcc_delete_plugin_test.cpp
    scheduler/scheduler_test.cpp
    server/mock_socket.hpp
    server/postgres_protocol_handler_test.cpp
    server/query_handler_test.cpp
    server/read_buffer_test.cpp
    server/result_serializer_test.cpp
    server/write_buffer_test.cpp
    sql/sql_identifier_resolver_test.cpp
    sql/sql_pipeline_statement_test.cpp
    sql/sql_pipeline_test.cpp
    sql/query_plan_cache_test.cpp
    sql/sql_translator_test.cpp
    sql/sqlite_testrunner/sqlite_testrunner_unencoded.cpp
    sql/sqlite_testrunner/sqlite_wrapper_test.cpp
    lossy_cast_test.cpp
    statistics/cardinality_estimator_test.cpp
    statistics/attribute_statistics_test.cpp
    statistics/join_graph_statistics_cache_test.cpp
    statistics/statistics_objects/equal_distinct_count_histogram_test.cpp
    statistics/statistics_objects/generic_histogram_test.cpp
    statistics/statistics_objects/string_histogram_domain_test.cpp
    statistics/statistics_objects/min_max_filter_test.cpp
    statistics/statistics_objects/counting_quotient_filter_test.cpp
    statistics/statistics_objects/range_filter_test.cpp
    statistics/table_statistics_test.cpp
    storage/adaptive_radix_tree_index_test.cpp
    storage/any_segment_iterable_test.cpp
    storage/btree_index_test.cpp
    storage/chunk_encoder_test.cpp
    storage/chunk_test.cpp
    storage/composite_group_key_index_test.cpp
    storage/compressed_vector_test.cpp
    storage/constraints_test.cpp
    storage/dictionary_segment_test.cpp
    storage/encoded_segment_test.cpp
    storage/encoded_string_segment_test.cpp
    storage/encoding_test.hpp
    storage/fixed_string_dictionary_segment_test.cpp
    storage/fixed_string_vector_test.cpp
    storage/group_key_index_test.cpp
    storage/iterables_test.cpp
    storage/lz4_segment_test.cpp
    storage/materialize_test.cpp
    storage/multi_segment_index_test.cpp
    storage/prepared_plan_test.cpp
    storage/reference_segment_test.cpp
    storage/segment_accessor_test.cpp
    storage/segment_iterators_test.cpp
    storage/simd_bp128_test.cpp
    storage/single_segment_index_test.cpp
    storage/storage_manager_test.cpp
    storage/table_test.cpp
    storage/table_column_definition_test.cpp
    storage/value_segment_test.cpp
    storage/variable_length_key_base_test.cpp
    storage/variable_length_key_store_test.cpp
    storage/variable_length_key_test.cpp
    tasks/chunk_compression_task_test.cpp
    tasks/operator_task_test.cpp
    testing_assert.cpp
    testing_assert.hpp
    utils/format_bytes_test.cpp
    utils/format_duration_test.cpp
    utils/lossless_predicate_cast_test.cpp
    utils/meta_table_manager_test.cpp
    utils/plugin_manager_test.cpp
    utils/plugin_test_utils.cpp
    utils/plugin_test_utils.hpp
    utils/column_ids_after_pruning_test.cpp
    utils/singleton_test.cpp
    utils/string_utils_test.cpp
)

set (
    SYSTEM_TEST_SOURCES
    ${SHARED_SOURCES}
    concurrency/stress_test.cpp
    server/server_test_runner.cpp
    sql/sqlite_testrunner/sqlite_testrunner_encodings.cpp
    tpc/tpcc_test.cpp
    synthetic_table_generator_test.cpp
    tpc/tpcds_db_generator_test.cpp
    tpc/tpch_db_generator_test.cpp
    gtest_main.cpp
    utils/plugin_test_utils.cpp
    utils/plugin_test_utils.hpp
    plugins/mvcc_delete_plugin_system_test.cpp
)

# Both hyriseTest and hyriseSystemTest link against these
set(
    LIBRARIES
    gtest
    gmock
    sqlite3
    MvccDeletePlugin  # So that we can test member methods without going through dlsym
)

# This warning does not play well with SCOPED_TRACE
add_compile_options(-Wno-used-but-marked-unused)

# We define TEST_PLUGIN_DIR to always load plugins from the correct directory for testing purposes
add_definitions(-DTEST_PLUGIN_DIR="${CMAKE_BINARY_DIR}/lib/")

# Build special sanitizer version of googletest
include_directories(../../third_party/googletest/googletest/)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Configure hyriseTest
add_executable(hyriseTest ${HYRISE_UNIT_TEST_SOURCES})
add_dependencies(hyriseTest hyriseTestPlugin hyriseTestNonInstantiablePlugin)
target_link_libraries(hyriseTest hyrise ${LIBRARIES})
target_link_libraries_system(hyriseTest nlohmann_json::nlohmann_json)

# Configure hyriseSystemTest
add_executable(hyriseSystemTest ${SYSTEM_TEST_SOURCES})
target_link_libraries(hyriseSystemTest hyrise hyriseBenchmarkLib ${LIBRARIES})
target_link_libraries_system(hyriseSystemTest nlohmann_json::nlohmann_json pqxx_static)
target_compile_options(hyriseSystemTest PRIVATE -DPQXX_HIDE_EXP_OPTIONAL)
