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
    operators/join_mpsm_test.cpp
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
    optimizer/strategy/chunk_pruning_test.cpp
    optimizer/strategy/column_pruning_rule_test.cpp
    optimizer/strategy/expression_reduction_rule_test.cpp
    optimizer/strategy/index_scan_rule_test.cpp
    optimizer/strategy/insert_limit_in_exists_rule_test.cpp
    optimizer/strategy/join_ordering_rule_test.cpp
    optimizer/strategy/predicate_placement_rule_test.cpp
    optimizer/strategy/predicate_reordering_rule_test.cpp
    optimizer/strategy/predicate_split_up_rule_test.cpp
    optimizer/strategy/strategy_base_test.cpp
    optimizer/strategy/strategy_base_test.hpp
    optimizer/strategy/subquery_to_join_rule_test.cpp
    scheduler/scheduler_test.cpp
    server/mock_connection.hpp
    server/mock_task_runner.hpp
    server/postgres_wire_handler_test.cpp
    server/server_session_test.cpp
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
    storage/value_segment_test.cpp
    storage/variable_length_key_base_test.cpp
    storage/variable_length_key_store_test.cpp
    storage/variable_length_key_test.cpp
    tasks/chunk_compression_task_test.cpp
    tasks/load_server_file_task_test.cpp
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
    server/server_test_runner.cpp
    sql/sqlite_testrunner/sqlite_testrunner_encodings.cpp
    tpc/tpcds_db_generator_test.cpp
    tpc/tpch_test.cpp
    tpc/tpch_db_generator_test.cpp
    gtest_main.cpp
)

if (${ENABLE_JIT_SUPPORT})
    # Some of the JIT test cases require pre-compiled LLVM bitcode.
    # To ensure this bitcode is always up-to-date and works across operating systems, the bitcode is automatically
    # compiled along with the hyriseTest binary (see cmake/EmbedLLVM.cmake for details of this process).
    # The C++ input files for the bitcode generation are located in
    # src/test/operators/jit_operator/specialization/modules/.
    # Each line below starting with "embed_llvm(" embeds on of these test files and exposes it as a different symbol
    # name. Each test case can the access the required bitcode using this symbol.
    include(${CMAKE_SOURCE_DIR}/cmake/EmbedLLVM.cmake)
    embed_llvm(
        JIT_CODE_SPECIALIZER_TEST_MODULE
        jit_code_specializer_test_module
        operators/jit_operator/specialization/modules/jit_code_specializer_test_module.cpp
    )
    embed_llvm(
        JIT_COMPILER_TEST_MODULE
        jit_compiler_test_module
        operators/jit_operator/specialization/modules/jit_compiler_test_module.cpp
    )
    embed_llvm(
        JIT_REPOSITORY_TEST_MODULE
        jit_repository_test_module
        operators/jit_operator/specialization/modules/jit_repository_test_module.cpp
    )
    embed_llvm(
        RESOLVE_CONDITION_TEST_MODULE
        resolve_condition_test_module
        operators/jit_operator/specialization/modules/resolve_condition_test_module.cpp
    )

    # Prevent clang from complaining about unused defines when compiling the assembly file
    set_property(
        SOURCE
        ${JIT_CODE_SPECIALIZER_TEST_MODULE}
        ${JIT_COMPILER_TEST_MODULE}
        ${JIT_REPOSITORY_TEST_MODULE}
        ${RESOLVE_CONDITION_TEST_MODULE}
        PROPERTY COMPILE_FLAGS -Qunused-arguments
    )

    set(HYRISE_UNIT_TEST_SOURCES
        ${HYRISE_UNIT_TEST_SOURCES}
        ${JIT_CODE_SPECIALIZER_TEST_MODULE}
        ${JIT_COMPILER_TEST_MODULE}
        ${JIT_REPOSITORY_TEST_MODULE}
        ${RESOLVE_CONDITION_TEST_MODULE}
        operators/jit_operator_wrapper_test.cpp
        logical_query_plan/jit_aware_lqp_translator_test.cpp
        operators/jit_operator/jit_hashmap_entry_test.cpp
        operators/jit_operator/jit_operations_test.cpp
        operators/jit_operator/jit_tuple_entry_test.cpp
        operators/jit_operator/jit_variant_vector_test.cpp
        operators/jit_operator/operators/jit_aggregate_test.cpp
        operators/jit_operator/operators/jit_compute_test.cpp
        operators/jit_operator/operators/jit_expression_test.cpp
        operators/jit_operator/operators/jit_filter_test.cpp
        operators/jit_operator/operators/jit_limit_test.cpp
        operators/jit_operator/operators/jit_read_write_tuple_test.cpp
        operators/jit_operator/operators/jit_validate_test.cpp
        operators/jit_operator/operators/jit_write_reference_test.cpp
        operators/jit_operator/specialization/get_runtime_pointer_for_value_test.cpp
        operators/jit_operator/specialization/jit_code_specializer_test.cpp
        operators/jit_operator/specialization/jit_compiler_test.cpp
        operators/jit_operator/specialization/jit_repository_test.cpp
        operators/jit_operator/specialization/jit_runtime_pointer_test.cpp
        operators/jit_operator/specialization/resolve_condition_test.cpp
        operators/jit_operator/specialization/modules/jit_code_specializer_test_module.cpp
        operators/jit_operator/specialization/modules/jit_code_specializer_test_module.hpp
        tasks/load_server_file_task_test.cpp)

    set(SYSTEM_TEST_SOURCES
        ${SYSTEM_TEST_SOURCES}
        sql/sqlite_testrunner/sqlite_testrunner_jit.cpp
    )
endif()

# Both hyriseTest and hyriseSystemTest link against these
set(
    LIBRARIES
    gtest
    gmock
    sqlite3
    ${FILESYSTEM_LIBRARY}
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

# Configure hyriseSystemTest
add_executable(hyriseSystemTest ${SYSTEM_TEST_SOURCES})
target_link_libraries(hyriseSystemTest hyrise hyriseBenchmarkLib ${LIBRARIES})
target_link_libraries_system(hyriseSystemTest pqxx)
target_compile_options(hyriseSystemTest PRIVATE -DPQXX_HIDE_EXP_OPTIONAL)
