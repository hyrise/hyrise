set(
    SHARED_SOURCES
    base_test.cpp
    base_test.hpp
    lib/sql/sqlite_testrunner/sqlite_testrunner.cpp
    lib/sql/sqlite_testrunner/sqlite_testrunner.hpp
    testing_assert.cpp
    testing_assert.hpp
)

set(
    HYRISE_UNIT_TEST_SOURCES
    ${SHARED_SOURCES}
    benchmarklib/sqlite_add_indices_test.cpp
    benchmarklib/table_builder_test.cpp
    gtest_case_template.cpp
    gtest_main.cpp
    lib/all_parameter_variant_test.cpp
    lib/all_type_variant_test.cpp
    lib/cache/cache_test.cpp
    lib/concurrency/commit_context_test.cpp
    lib/concurrency/transaction_context_test.cpp
    lib/concurrency/transaction_manager_test.cpp
    lib/cost_estimation/abstract_cost_estimator_test.cpp
    lib/expression/evaluation/expression_result_test.cpp
    lib/expression/evaluation/like_matcher_test.cpp
    lib/expression/expression_evaluator_to_pos_list_test.cpp
    lib/expression/expression_evaluator_to_values_test.cpp
    lib/expression/expression_test.cpp
    lib/expression/expression_utils_test.cpp
    lib/expression/lqp_subquery_expression_test.cpp
    lib/expression/pqp_subquery_expression_test.cpp
    lib/hyrise_test.cpp
    lib/import_export/binary/binary_parser_test.cpp
    lib/import_export/binary/binary_writer_test.cpp
    lib/import_export/csv/csv_meta_test.cpp
    lib/import_export/csv/csv_parser_test.cpp
    lib/import_export/csv/csv_writer_test.cpp
    lib/logical_query_plan/aggregate_node_test.cpp
    lib/logical_query_plan/alias_node_test.cpp
    lib/logical_query_plan/change_meta_table_node_test.cpp
    lib/logical_query_plan/create_prepared_plan_node_test.cpp
    lib/logical_query_plan/create_table_node_test.cpp
    lib/logical_query_plan/create_view_node_test.cpp
    lib/logical_query_plan/delete_node_test.cpp
    lib/logical_query_plan/drop_table_node_test.cpp
    lib/logical_query_plan/drop_view_node_test.cpp
    lib/logical_query_plan/dummy_table_node_test.cpp
    lib/logical_query_plan/except_node_test.cpp
    lib/logical_query_plan/export_node_test.cpp
    lib/logical_query_plan/functional_dependency_test.cpp
    lib/logical_query_plan/import_node_test.cpp
    lib/logical_query_plan/insert_node_test.cpp
    lib/logical_query_plan/intersect_node_test.cpp
    lib/logical_query_plan/join_node_test.cpp
    lib/logical_query_plan/limit_node_test.cpp
    lib/logical_query_plan/logical_query_plan_test.cpp
    lib/logical_query_plan/lqp_find_subplan_mismatch_test.cpp
    lib/logical_query_plan/lqp_translator_test.cpp
    lib/logical_query_plan/lqp_unique_constraint_test.cpp
    lib/logical_query_plan/lqp_utils_test.cpp
    lib/logical_query_plan/mock_node_test.cpp
    lib/logical_query_plan/predicate_node_test.cpp
    lib/logical_query_plan/projection_node_test.cpp
    lib/logical_query_plan/sort_node_test.cpp
    lib/logical_query_plan/static_table_node_test.cpp
    lib/logical_query_plan/stored_table_node_test.cpp
    lib/logical_query_plan/union_node_test.cpp
    lib/logical_query_plan/update_node_test.cpp
    lib/logical_query_plan/validate_node_test.cpp
    lib/lossless_cast_test.cpp
    lib/lossy_cast_test.cpp
    lib/memory/segments_using_allocators_test.cpp
    lib/memory/zero_allocator_test.cpp
    lib/null_value_test.cpp
    lib/operators/aggregate_sort_test.cpp
    lib/operators/aggregate_test.cpp
    lib/operators/alias_operator_test.cpp
    lib/operators/change_meta_table_test.cpp
    lib/operators/delete_test.cpp
    lib/operators/difference_test.cpp
    lib/operators/export_test.cpp
    lib/operators/get_table_test.cpp
    lib/operators/import_test.cpp
    lib/operators/index_scan_test.cpp
    lib/operators/insert_test.cpp
    lib/operators/join_hash/join_hash_steps_test.cpp
    lib/operators/join_hash/join_hash_traits_test.cpp
    lib/operators/join_hash/join_hash_types_test.cpp
    lib/operators/join_hash_test.cpp
    lib/operators/join_index_test.cpp
    lib/operators/join_nested_loop_test.cpp
    lib/operators/join_sort_merge_test.cpp
    lib/operators/join_test_runner.cpp
    lib/operators/join_verification_test.cpp
    lib/operators/limit_test.cpp
    lib/operators/maintenance/create_prepared_plan_test.cpp
    lib/operators/maintenance/create_table_test.cpp
    lib/operators/maintenance/create_view_test.cpp
    lib/operators/maintenance/drop_table_test.cpp
    lib/operators/maintenance/drop_view_test.cpp
    lib/operators/operator_clear_output_test.cpp
    lib/operators/operator_deep_copy_test.cpp
    lib/operators/operator_join_predicate_test.cpp
    lib/operators/operator_performance_data_test.cpp
    lib/operators/operator_scan_predicate_test.cpp
    lib/operators/pqp_utils_test.cpp
    lib/operators/print_test.cpp
    lib/operators/product_test.cpp
    lib/operators/projection_test.cpp
    lib/operators/sort_test.cpp
    lib/operators/table_scan_between_test.cpp
    lib/operators/table_scan_sorted_segment_search_test.cpp
    lib/operators/table_scan_string_test.cpp
    lib/operators/table_scan_test.cpp
    lib/operators/typed_operator_base_test.hpp
    lib/operators/union_all_test.cpp
    lib/operators/union_positions_test.cpp
    lib/operators/update_test.cpp
    lib/operators/validate_test.cpp
    lib/operators/validate_visibility_test.cpp
    lib/optimizer/join_ordering/dp_ccp_test.cpp
    lib/optimizer/join_ordering/enumerate_ccp_test.cpp
    lib/optimizer/join_ordering/greedy_operator_ordering_test.cpp
    lib/optimizer/join_ordering/join_graph_builder_test.cpp
    lib/optimizer/join_ordering/join_graph_test.cpp
    lib/optimizer/optimizer_test.cpp
    lib/optimizer/strategy/between_composition_rule_test.cpp
    lib/optimizer/strategy/chunk_pruning_rule_test.cpp
    lib/optimizer/strategy/column_pruning_rule_test.cpp
    lib/optimizer/strategy/dependent_group_by_reduction_rule_test.cpp
    lib/optimizer/strategy/expression_reduction_rule_test.cpp
    lib/optimizer/strategy/in_expression_rewrite_rule_test.cpp
    lib/optimizer/strategy/index_scan_rule_test.cpp
    lib/optimizer/strategy/join_ordering_rule_test.cpp
    lib/optimizer/strategy/join_predicate_ordering_rule_test.cpp
    lib/optimizer/strategy/null_scan_removal_rule_test.cpp
    lib/optimizer/strategy/predicate_merge_rule_test.cpp
    lib/optimizer/strategy/predicate_placement_rule_test.cpp
    lib/optimizer/strategy/predicate_reordering_rule_test.cpp
    lib/optimizer/strategy/predicate_split_up_rule_test.cpp
    lib/optimizer/strategy/semi_join_reduction_rule_test.cpp
    lib/optimizer/strategy/stored_table_column_alignment_rule_test.cpp
    lib/optimizer/strategy/strategy_base_test.cpp
    lib/optimizer/strategy/strategy_base_test.hpp
    lib/optimizer/strategy/subquery_to_join_rule_test.cpp
    lib/scheduler/operator_task_test.cpp
    lib/scheduler/scheduler_test.cpp
    lib/server/mock_socket.hpp
    lib/server/postgres_protocol_handler_test.cpp
    lib/server/query_handler_test.cpp
    lib/server/read_buffer_test.cpp
    lib/server/result_serializer_test.cpp
    lib/server/transaction_handling_test.cpp
    lib/server/write_buffer_test.cpp
    lib/sql/sql_identifier_resolver_test.cpp
    lib/sql/sql_pipeline_statement_test.cpp
    lib/sql/sql_pipeline_test.cpp
    lib/sql/sql_plan_cache_test.cpp
    lib/sql/sql_translator_test.cpp
    lib/sql/sqlite_testrunner/sqlite_testrunner_unencoded.cpp
    lib/sql/sqlite_testrunner/sqlite_wrapper_test.cpp
    lib/statistics/attribute_statistics_test.cpp
    lib/statistics/cardinality_estimator_test.cpp
    lib/statistics/join_graph_statistics_cache_test.cpp
    lib/statistics/statistics_objects/equal_distinct_count_histogram_test.cpp
    lib/statistics/statistics_objects/generic_histogram_test.cpp
    lib/statistics/statistics_objects/min_max_filter_test.cpp
    lib/statistics/statistics_objects/range_filter_test.cpp
    lib/statistics/statistics_objects/string_histogram_domain_test.cpp
    lib/statistics/table_statistics_test.cpp
    lib/storage/any_segment_iterable_test.cpp
    lib/storage/chunk_encoder_test.cpp
    lib/storage/chunk_test.cpp
    lib/storage/compressed_vector_test.cpp
    lib/storage/dictionary_segment_test.cpp
    lib/storage/encoded_segment_test.cpp
    lib/storage/encoded_string_segment_test.cpp
    lib/storage/encoding_test.hpp
    lib/storage/fixed_string_dictionary_segment/fixed_string_test.cpp
    lib/storage/fixed_string_dictionary_segment/fixed_string_vector_test.cpp
    lib/storage/fixed_string_dictionary_segment_test.cpp
    lib/storage/index/adaptive_radix_tree/adaptive_radix_tree_index_test.cpp
    lib/storage/index/b_tree/b_tree_index_test.cpp
    lib/storage/index/group_key/composite_group_key_index_test.cpp
    lib/storage/index/group_key/group_key_index_test.cpp
    lib/storage/index/group_key/variable_length_key_base_test.cpp
    lib/storage/index/group_key/variable_length_key_store_test.cpp
    lib/storage/index/group_key/variable_length_key_test.cpp
    lib/storage/index/multi_segment_index_test.cpp
    lib/storage/index/single_segment_index_test.cpp
    lib/storage/iterables_test.cpp
    lib/storage/lz4_segment_test.cpp
    lib/storage/materialize_test.cpp
    lib/storage/pos_lists/entire_chunk_pos_list_test.cpp
    lib/storage/prepared_plan_test.cpp
    lib/storage/reference_segment_test.cpp
    lib/storage/segment_access_counter_test.cpp
    lib/storage/segment_accessor_test.cpp
    lib/storage/segment_iterators_test.cpp
    lib/storage/storage_manager_test.cpp
    lib/storage/table_column_definition_test.cpp
    lib/storage/table_key_constraint_test.cpp
    lib/storage/table_test.cpp
    lib/storage/value_segment_test.cpp
    lib/tasks/chunk_compression_task_test.cpp
    lib/utils/check_table_equal_test.cpp
    lib/utils/column_ids_after_pruning_test.cpp
    lib/utils/date_utils_test.cpp
    lib/utils/format_bytes_test.cpp
    lib/utils/format_duration_test.cpp
    lib/utils/load_table_test.cpp
    lib/utils/log_manager_test.cpp
    lib/utils/lossless_predicate_cast_test.cpp
    lib/utils/meta_table_manager_test.cpp
    lib/utils/meta_tables/meta_exec_table_test.cpp
    lib/utils/meta_tables/meta_log_table_test.cpp
    lib/utils/meta_tables/meta_mock_table.cpp
    lib/utils/meta_tables/meta_mock_table.hpp
    lib/utils/meta_tables/meta_plugins_table_test.cpp
    lib/utils/meta_tables/meta_settings_table_test.cpp
    lib/utils/meta_tables/meta_system_utilization_table_test.cpp
    lib/utils/meta_tables/meta_table_test.cpp
    lib/utils/mock_setting.cpp
    lib/utils/mock_setting.hpp
    lib/utils/plugin_manager_test.cpp
    lib/utils/plugin_test_utils.cpp
    lib/utils/plugin_test_utils.hpp
    lib/utils/setting_test.cpp
    lib/utils/settings_manager_test.cpp
    lib/utils/singleton_test.cpp
    lib/utils/size_estimation_utils_test.cpp
    lib/utils/string_utils_test.cpp
    plugins/mvcc_delete_plugin_test.cpp
    testing_assert.cpp
    testing_assert.hpp
    utils/constraint_test_utils.hpp
)

set (
    SYSTEM_TEST_SOURCES
    ${SHARED_SOURCES}
    benchmarklib/synthetic_table_generator_test.cpp
    benchmarklib/tpcc/tpcc_test.cpp
    benchmarklib/tpcds/tpcds_db_generator_test.cpp
    benchmarklib/tpch/tpch_db_generator_test.cpp
    gtest_main.cpp
    lib/concurrency/stress_test.cpp
    lib/server/server_test_runner.cpp
    lib/sql/sqlite_testrunner/sqlite_testrunner_encodings.cpp
    lib/utils/plugin_test_utils.cpp
    lib/utils/plugin_test_utils.hpp
    plugins/mvcc_delete_plugin_system_test.cpp
)

# Both hyriseTest and hyriseSystemTest link against these
set(
    LIBRARIES
    gtest
    gmock
    SQLite::SQLite3
    hyriseMvccDeletePlugin  # So that we can test member methods without going through dlsym
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
add_dependencies(hyriseTest hyriseSecondTestPlugin hyriseTestPlugin hyriseMvccDeletePlugin hyriseTestNonInstantiablePlugin)
target_link_libraries(hyriseTest hyrise ${LIBRARIES})

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
target_compile_options(hyriseTest PUBLIC -Xclang -fno-pch-timestamp)
endif()
if(NOT ${CMAKE_VERSION} VERSION_LESS "3.16.0")
    target_precompile_headers(hyriseTest PRIVATE
        <gtest/gtest.h>
        [["all_parameter_variant.hpp"]]
        [["storage/create_iterable_from_segment.hpp"]]
        [["storage/table.hpp"]]
        [["types.hpp"]]
    )
endif()

# Configure hyriseSystemTest
add_executable(hyriseSystemTest ${SYSTEM_TEST_SOURCES})
target_link_libraries(hyriseSystemTest PUBLIC hyrise hyriseBenchmarkLib ${LIBRARIES})
target_link_libraries_system(hyriseSystemTest pqxx_static)
target_compile_options(hyriseSystemTest PRIVATE -DPQXX_HIDE_EXP_OPTIONAL)
