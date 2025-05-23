# Setup the version.hpp file.
# Include this here because we need the GENERATED property on version.hpp and in CMake that property has directory
# scope
include(${PROJECT_SOURCE_DIR}/cmake/git_watcher.cmake)
# Sources and libraries shared among the different builds of the lib
set(
    SOURCES
    all_parameter_variant.cpp
    all_parameter_variant.hpp
    all_type_variant.cpp
    all_type_variant.hpp
    cache/abstract_cache.hpp
    cache/gdfs_cache.hpp
    concurrency/commit_context.cpp
    concurrency/commit_context.hpp
    concurrency/transaction_context.cpp
    concurrency/transaction_context.hpp
    concurrency/transaction_manager.cpp
    concurrency/transaction_manager.hpp
    cost_estimation/abstract_cost_estimator.cpp
    cost_estimation/abstract_cost_estimator.hpp
    cost_estimation/cost_estimator_logical.cpp
    cost_estimation/cost_estimator_logical.hpp
    expression/abstract_expression.cpp
    expression/abstract_expression.hpp
    expression/abstract_predicate_expression.cpp
    expression/abstract_predicate_expression.hpp
    expression/arithmetic_expression.cpp
    expression/arithmetic_expression.hpp
    expression/between_expression.cpp
    expression/between_expression.hpp
    expression/binary_predicate_expression.cpp
    expression/binary_predicate_expression.hpp
    expression/case_expression.cpp
    expression/case_expression.hpp
    expression/cast_expression.cpp
    expression/cast_expression.hpp
    expression/correlated_parameter_expression.cpp
    expression/correlated_parameter_expression.hpp
    expression/evaluation/expression_evaluator.cpp
    expression/evaluation/expression_evaluator.hpp
    expression/evaluation/expression_functors.hpp
    expression/evaluation/expression_result.hpp
    expression/evaluation/expression_result_views.hpp
    expression/evaluation/like_matcher.cpp
    expression/evaluation/like_matcher.hpp
    expression/exists_expression.cpp
    expression/exists_expression.hpp
    expression/expression_functional.cpp
    expression/expression_functional.hpp
    expression/expression_precedence.hpp
    expression/expression_utils.cpp
    expression/expression_utils.hpp
    expression/extract_expression.cpp
    expression/extract_expression.hpp
    expression/function_expression.cpp
    expression/function_expression.hpp
    expression/in_expression.cpp
    expression/in_expression.hpp
    expression/interval_expression.cpp
    expression/interval_expression.hpp
    expression/is_null_expression.cpp
    expression/is_null_expression.hpp
    expression/list_expression.cpp
    expression/list_expression.hpp
    expression/logical_expression.cpp
    expression/logical_expression.hpp
    expression/lqp_column_expression.cpp
    expression/lqp_column_expression.hpp
    expression/lqp_subquery_expression.cpp
    expression/lqp_subquery_expression.hpp
    expression/placeholder_expression.cpp
    expression/placeholder_expression.hpp
    expression/pqp_column_expression.cpp
    expression/pqp_column_expression.hpp
    expression/pqp_subquery_expression.cpp
    expression/pqp_subquery_expression.hpp
    expression/unary_minus_expression.cpp
    expression/unary_minus_expression.hpp
    expression/value_expression.cpp
    expression/value_expression.hpp
    expression/window_expression.cpp
    expression/window_expression.hpp
    expression/window_function_expression.cpp
    expression/window_function_expression.hpp
    hyrise.cpp
    hyrise.hpp
    import_export/binary/binary_parser.cpp
    import_export/binary/binary_parser.hpp
    import_export/binary/binary_writer.cpp
    import_export/binary/binary_writer.hpp
    import_export/csv/csv_converter.cpp
    import_export/csv/csv_converter.hpp
    import_export/csv/csv_meta.cpp
    import_export/csv/csv_meta.hpp
    import_export/csv/csv_parser.cpp
    import_export/csv/csv_parser.hpp
    import_export/csv/csv_writer.cpp
    import_export/csv/csv_writer.hpp
    import_export/file_type.cpp
    import_export/file_type.hpp
    logical_query_plan/abstract_lqp_node.cpp
    logical_query_plan/abstract_lqp_node.hpp
    logical_query_plan/abstract_non_query_node.cpp
    logical_query_plan/abstract_non_query_node.hpp
    logical_query_plan/aggregate_node.cpp
    logical_query_plan/aggregate_node.hpp
    logical_query_plan/alias_node.cpp
    logical_query_plan/alias_node.hpp
    logical_query_plan/change_meta_table_node.cpp
    logical_query_plan/change_meta_table_node.hpp
    logical_query_plan/create_prepared_plan_node.cpp
    logical_query_plan/create_prepared_plan_node.hpp
    logical_query_plan/create_table_node.cpp
    logical_query_plan/create_table_node.hpp
    logical_query_plan/create_view_node.cpp
    logical_query_plan/create_view_node.hpp
    logical_query_plan/data_dependencies/functional_dependency.cpp
    logical_query_plan/data_dependencies/functional_dependency.hpp
    logical_query_plan/data_dependencies/order_dependency.cpp
    logical_query_plan/data_dependencies/order_dependency.hpp
    logical_query_plan/data_dependencies/unique_column_combination.cpp
    logical_query_plan/data_dependencies/unique_column_combination.hpp
    logical_query_plan/delete_node.cpp
    logical_query_plan/delete_node.hpp
    logical_query_plan/drop_table_node.cpp
    logical_query_plan/drop_table_node.hpp
    logical_query_plan/drop_view_node.cpp
    logical_query_plan/drop_view_node.hpp
    logical_query_plan/dummy_table_node.cpp
    logical_query_plan/dummy_table_node.hpp
    logical_query_plan/enable_make_for_lqp_node.hpp
    logical_query_plan/except_node.cpp
    logical_query_plan/except_node.hpp
    logical_query_plan/export_node.cpp
    logical_query_plan/export_node.hpp
    logical_query_plan/import_node.cpp
    logical_query_plan/import_node.hpp
    logical_query_plan/insert_node.cpp
    logical_query_plan/insert_node.hpp
    logical_query_plan/intersect_node.cpp
    logical_query_plan/intersect_node.hpp
    logical_query_plan/join_node.cpp
    logical_query_plan/join_node.hpp
    logical_query_plan/limit_node.cpp
    logical_query_plan/limit_node.hpp
    logical_query_plan/logical_plan_root_node.cpp
    logical_query_plan/logical_plan_root_node.hpp
    logical_query_plan/lqp_translator.cpp
    logical_query_plan/lqp_translator.hpp
    logical_query_plan/lqp_utils.cpp
    logical_query_plan/lqp_utils.hpp
    logical_query_plan/mock_node.cpp
    logical_query_plan/mock_node.hpp
    logical_query_plan/predicate_node.cpp
    logical_query_plan/predicate_node.hpp
    logical_query_plan/projection_node.cpp
    logical_query_plan/projection_node.hpp
    logical_query_plan/sort_node.cpp
    logical_query_plan/sort_node.hpp
    logical_query_plan/static_table_node.cpp
    logical_query_plan/static_table_node.hpp
    logical_query_plan/stored_table_node.cpp
    logical_query_plan/stored_table_node.hpp
    logical_query_plan/union_node.cpp
    logical_query_plan/union_node.hpp
    logical_query_plan/update_node.cpp
    logical_query_plan/update_node.hpp
    logical_query_plan/validate_node.cpp
    logical_query_plan/validate_node.hpp
    logical_query_plan/window_node.cpp
    logical_query_plan/window_node.hpp
    lossless_cast.cpp
    lossless_cast.hpp
    lossy_cast.hpp
    memory/default_memory_resource.cpp
    memory/default_memory_resource.hpp
    memory/zero_allocator.hpp
    null_value.hpp
    operators/abstract_aggregate_operator.cpp
    operators/abstract_aggregate_operator.hpp
    operators/abstract_join_operator.cpp
    operators/abstract_join_operator.hpp
    operators/abstract_operator.cpp
    operators/abstract_operator.hpp
    operators/abstract_read_only_operator.cpp
    operators/abstract_read_only_operator.hpp
    operators/abstract_read_write_operator.cpp
    operators/abstract_read_write_operator.hpp
    operators/aggregate/window_function_traits.hpp
    operators/aggregate_hash.cpp
    operators/aggregate_hash.hpp
    operators/aggregate_sort.cpp
    operators/aggregate_sort.hpp
    operators/alias_operator.cpp
    operators/alias_operator.hpp
    operators/change_meta_table.cpp
    operators/change_meta_table.hpp
    operators/delete.cpp
    operators/delete.hpp
    operators/difference.cpp
    operators/difference.hpp
    operators/export.cpp
    operators/export.hpp
    operators/get_table.cpp
    operators/get_table.hpp
    operators/import.cpp
    operators/import.hpp
    operators/index_scan.cpp
    operators/index_scan.hpp
    operators/insert.cpp
    operators/insert.hpp
    operators/join_helper/join_output_writing.cpp
    operators/join_helper/join_output_writing.hpp
    operators/join_hash.cpp
    operators/join_hash.hpp
    operators/join_hash/join_hash_steps.hpp
    operators/join_hash/join_hash_traits.hpp
    operators/join_index.cpp
    operators/join_index.hpp
    operators/join_nested_loop.cpp
    operators/join_nested_loop.hpp
    operators/join_sort_merge.cpp
    operators/join_sort_merge.hpp
    operators/join_sort_merge/column_materializer.hpp
    operators/join_sort_merge/radix_cluster_sort.hpp
    operators/join_verification.cpp
    operators/join_verification.hpp
    operators/limit.cpp
    operators/limit.hpp
    operators/maintenance/create_prepared_plan.cpp
    operators/maintenance/create_prepared_plan.hpp
    operators/maintenance/create_table.cpp
    operators/maintenance/create_table.hpp
    operators/maintenance/create_view.cpp
    operators/maintenance/create_view.hpp
    operators/maintenance/drop_table.cpp
    operators/maintenance/drop_table.hpp
    operators/maintenance/drop_view.cpp
    operators/maintenance/drop_view.hpp
    operators/multi_predicate_join/multi_predicate_join_evaluator.cpp
    operators/multi_predicate_join/multi_predicate_join_evaluator.hpp
    operators/operator_join_predicate.cpp
    operators/operator_join_predicate.hpp
    operators/operator_performance_data.cpp
    operators/operator_performance_data.hpp
    operators/operator_scan_predicate.cpp
    operators/operator_scan_predicate.hpp
    operators/pqp_utils.cpp
    operators/pqp_utils.hpp
    operators/print.cpp
    operators/print.hpp
    operators/product.cpp
    operators/product.hpp
    operators/projection.cpp
    operators/projection.hpp
    operators/sort.cpp
    operators/sort.hpp
    operators/table_scan.cpp
    operators/table_scan.hpp
    operators/table_scan/abstract_dereferenced_column_table_scan_impl.cpp
    operators/table_scan/abstract_dereferenced_column_table_scan_impl.hpp
    operators/table_scan/abstract_table_scan_impl.hpp
    operators/table_scan/column_between_table_scan_impl.cpp
    operators/table_scan/column_between_table_scan_impl.hpp
    operators/table_scan/column_is_null_table_scan_impl.cpp
    operators/table_scan/column_is_null_table_scan_impl.hpp
    operators/table_scan/column_like_table_scan_impl.cpp
    operators/table_scan/column_like_table_scan_impl.hpp
    operators/table_scan/column_vs_column_table_scan_impl.cpp
    operators/table_scan/column_vs_column_table_scan_impl.hpp
    operators/table_scan/column_vs_value_table_scan_impl.cpp
    operators/table_scan/column_vs_value_table_scan_impl.hpp
    operators/table_scan/expression_evaluator_table_scan_impl.cpp
    operators/table_scan/expression_evaluator_table_scan_impl.hpp
    operators/table_scan/sorted_segment_search.hpp
    operators/table_wrapper.cpp
    operators/table_wrapper.hpp
    operators/union_all.cpp
    operators/union_all.hpp
    operators/union_positions.cpp
    operators/union_positions.hpp
    operators/update.cpp
    operators/update.hpp
    operators/validate.cpp
    operators/validate.hpp
    optimizer/join_ordering/abstract_join_ordering_algorithm.cpp
    optimizer/join_ordering/abstract_join_ordering_algorithm.hpp
    optimizer/join_ordering/dp_ccp.cpp
    optimizer/join_ordering/dp_ccp.hpp
    optimizer/join_ordering/enumerate_ccp.cpp
    optimizer/join_ordering/enumerate_ccp.hpp
    optimizer/join_ordering/greedy_operator_ordering.cpp
    optimizer/join_ordering/greedy_operator_ordering.hpp
    optimizer/join_ordering/join_graph.cpp
    optimizer/join_ordering/join_graph.hpp
    optimizer/join_ordering/join_graph_builder.cpp
    optimizer/join_ordering/join_graph_builder.hpp
    optimizer/join_ordering/join_graph_edge.cpp
    optimizer/join_ordering/join_graph_edge.hpp
    optimizer/optimizer.cpp
    optimizer/optimizer.hpp
    optimizer/strategy/abstract_rule.cpp
    optimizer/strategy/abstract_rule.hpp
    optimizer/strategy/between_composition_rule.cpp
    optimizer/strategy/between_composition_rule.hpp
    optimizer/strategy/chunk_pruning_rule.cpp
    optimizer/strategy/chunk_pruning_rule.hpp
    optimizer/strategy/column_pruning_rule.cpp
    optimizer/strategy/column_pruning_rule.hpp
    optimizer/strategy/dependent_group_by_reduction_rule.cpp
    optimizer/strategy/dependent_group_by_reduction_rule.hpp
    optimizer/strategy/expression_reduction_rule.cpp
    optimizer/strategy/expression_reduction_rule.hpp
    optimizer/strategy/in_expression_rewrite_rule.cpp
    optimizer/strategy/in_expression_rewrite_rule.hpp
    optimizer/strategy/index_scan_rule.cpp
    optimizer/strategy/index_scan_rule.hpp
    optimizer/strategy/join_ordering_rule.cpp
    optimizer/strategy/join_ordering_rule.hpp
    optimizer/strategy/join_predicate_ordering_rule.cpp
    optimizer/strategy/join_predicate_ordering_rule.hpp
    optimizer/strategy/join_to_predicate_rewrite_rule.cpp
    optimizer/strategy/join_to_predicate_rewrite_rule.hpp
    optimizer/strategy/join_to_semi_join_rule.cpp
    optimizer/strategy/join_to_semi_join_rule.hpp
    optimizer/strategy/null_scan_removal_rule.cpp
    optimizer/strategy/null_scan_removal_rule.hpp
    optimizer/strategy/predicate_merge_rule.cpp
    optimizer/strategy/predicate_merge_rule.hpp
    optimizer/strategy/predicate_placement_rule.cpp
    optimizer/strategy/predicate_placement_rule.hpp
    optimizer/strategy/predicate_reordering_rule.cpp
    optimizer/strategy/predicate_reordering_rule.hpp
    optimizer/strategy/predicate_split_up_rule.cpp
    optimizer/strategy/predicate_split_up_rule.hpp
    optimizer/strategy/semi_join_reduction_rule.cpp
    optimizer/strategy/semi_join_reduction_rule.hpp
    optimizer/strategy/stored_table_column_alignment_rule.cpp
    optimizer/strategy/stored_table_column_alignment_rule.hpp
    optimizer/strategy/subquery_to_join_rule.cpp
    optimizer/strategy/subquery_to_join_rule.hpp
    resolve_type.hpp
    scheduler/abstract_scheduler.cpp
    scheduler/abstract_scheduler.hpp
    scheduler/abstract_task.cpp
    scheduler/abstract_task.hpp
    scheduler/immediate_execution_scheduler.cpp
    scheduler/immediate_execution_scheduler.hpp
    scheduler/job_task.cpp
    scheduler/job_task.hpp
    scheduler/node_queue_scheduler.cpp
    scheduler/node_queue_scheduler.hpp
    scheduler/operator_task.cpp
    scheduler/operator_task.hpp
    scheduler/shutdown_task.cpp
    scheduler/shutdown_task.hpp
    scheduler/task_queue.cpp
    scheduler/task_queue.hpp
    scheduler/task_utils.hpp
    scheduler/topology.cpp
    scheduler/topology.hpp
    scheduler/worker.cpp
    scheduler/worker.hpp
    server/client_disconnect_exception.hpp
    server/postgres_message_type.hpp
    server/postgres_protocol_handler.cpp
    server/postgres_protocol_handler.hpp
    server/query_handler.cpp
    server/query_handler.hpp
    server/read_buffer.cpp
    server/read_buffer.hpp
    server/result_serializer.cpp
    server/result_serializer.hpp
    server/ring_buffer_iterator.hpp
    server/server.cpp
    server/server.hpp
    server/server_types.hpp
    server/session.cpp
    server/session.hpp
    server/write_buffer.cpp
    server/write_buffer.hpp
    sql/create_sql_parser_error_message.cpp
    sql/create_sql_parser_error_message.hpp
    sql/parameter_id_allocator.cpp
    sql/parameter_id_allocator.hpp
    sql/sql_identifier.cpp
    sql/sql_identifier.hpp
    sql/sql_identifier_resolver.cpp
    sql/sql_identifier_resolver.hpp
    sql/sql_identifier_resolver_proxy.cpp
    sql/sql_identifier_resolver_proxy.hpp
    sql/sql_pipeline.cpp
    sql/sql_pipeline.hpp
    sql/sql_pipeline_builder.cpp
    sql/sql_pipeline_builder.hpp
    sql/sql_pipeline_statement.cpp
    sql/sql_pipeline_statement.hpp
    sql/sql_plan_cache.hpp
    sql/sql_translator.cpp
    sql/sql_translator.hpp
    statistics/attribute_statistics.cpp
    statistics/attribute_statistics.hpp
    statistics/base_attribute_statistics.cpp
    statistics/base_attribute_statistics.hpp
    statistics/cardinality_estimation_cache.hpp
    statistics/cardinality_estimator.cpp
    statistics/cardinality_estimator.hpp
    statistics/generate_pruning_statistics.cpp
    statistics/generate_pruning_statistics.hpp
    statistics/join_graph_statistics_cache.cpp
    statistics/join_graph_statistics_cache.hpp
    statistics/statistics_objects/abstract_histogram.cpp
    statistics/statistics_objects/abstract_histogram.hpp
    statistics/statistics_objects/abstract_statistics_object.cpp
    statistics/statistics_objects/abstract_statistics_object.hpp
    statistics/statistics_objects/distinct_value_count.cpp
    statistics/statistics_objects/distinct_value_count.hpp
    statistics/statistics_objects/equal_distinct_count_histogram.cpp
    statistics/statistics_objects/equal_distinct_count_histogram.hpp
    statistics/statistics_objects/generic_histogram.cpp
    statistics/statistics_objects/generic_histogram.hpp
    statistics/statistics_objects/generic_histogram_builder.cpp
    statistics/statistics_objects/generic_histogram_builder.hpp
    statistics/statistics_objects/histogram_domain.cpp
    statistics/statistics_objects/histogram_domain.hpp
    statistics/statistics_objects/min_max_filter.cpp
    statistics/statistics_objects/min_max_filter.hpp
    statistics/statistics_objects/null_value_ratio_statistics.cpp
    statistics/statistics_objects/null_value_ratio_statistics.hpp
    statistics/statistics_objects/range_filter.cpp
    statistics/statistics_objects/range_filter.hpp
    statistics/statistics_objects/scaled_histogram.cpp
    statistics/statistics_objects/scaled_histogram.hpp
    statistics/table_statistics.cpp
    statistics/table_statistics.hpp
    storage/abstract_encoded_segment.hpp
    storage/abstract_segment.cpp
    storage/abstract_segment.hpp
    storage/base_dictionary_segment.hpp
    storage/base_segment_accessor.hpp
    storage/base_segment_encoder.hpp
    storage/base_value_segment.hpp
    storage/buffer/frame.cpp
    storage/buffer/frame.hpp
    storage/buffer/page_id.hpp
    storage/chunk.cpp
    storage/chunk.hpp
    storage/chunk_encoder.cpp
    storage/chunk_encoder.hpp
    storage/constraints/abstract_table_constraint.cpp
    storage/constraints/abstract_table_constraint.hpp
    storage/constraints/constraint_utils.cpp
    storage/constraints/constraint_utils.hpp
    storage/constraints/foreign_key_constraint.cpp
    storage/constraints/foreign_key_constraint.hpp
    storage/constraints/table_key_constraint.cpp
    storage/constraints/table_key_constraint.hpp
    storage/constraints/table_order_constraint.cpp
    storage/constraints/table_order_constraint.hpp
    storage/create_iterable_from_reference_segment.ipp
    storage/create_iterable_from_segment.hpp
    storage/create_iterable_from_segment.ipp
    storage/dictionary_segment.cpp
    storage/dictionary_segment.hpp
    storage/dictionary_segment/attribute_vector_iterable.hpp
    storage/dictionary_segment/dictionary_encoder.hpp
    storage/dictionary_segment/dictionary_segment_iterable.hpp
    storage/encoding_type.cpp
    storage/encoding_type.hpp
    storage/fixed_string_dictionary_segment.cpp
    storage/fixed_string_dictionary_segment.hpp
    storage/fixed_string_dictionary_segment/fixed_string.cpp
    storage/fixed_string_dictionary_segment/fixed_string.hpp
    storage/fixed_string_dictionary_segment/fixed_string_vector.cpp
    storage/fixed_string_dictionary_segment/fixed_string_vector.hpp
    storage/fixed_string_dictionary_segment/fixed_string_vector_iterator.hpp
    storage/frame_of_reference_segment.cpp
    storage/frame_of_reference_segment.hpp
    storage/frame_of_reference_segment/frame_of_reference_encoder.hpp
    storage/frame_of_reference_segment/frame_of_reference_segment_iterable.hpp
    storage/index/abstract_chunk_index.cpp
    storage/index/abstract_chunk_index.hpp
    storage/index/adaptive_radix_tree/adaptive_radix_tree_index.cpp
    storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp
    storage/index/adaptive_radix_tree/adaptive_radix_tree_nodes.cpp
    storage/index/adaptive_radix_tree/adaptive_radix_tree_nodes.hpp
    storage/index/group_key/composite_group_key_index.cpp
    storage/index/group_key/composite_group_key_index.hpp
    storage/index/group_key/group_key_index.cpp
    storage/index/group_key/group_key_index.hpp
    storage/index/group_key/variable_length_key.cpp
    storage/index/group_key/variable_length_key.hpp
    storage/index/group_key/variable_length_key_base.cpp
    storage/index/group_key/variable_length_key_base.hpp
    storage/index/group_key/variable_length_key_proxy.cpp
    storage/index/group_key/variable_length_key_proxy.hpp
    storage/index/group_key/variable_length_key_store.cpp
    storage/index/group_key/variable_length_key_store.hpp
    storage/index/chunk_index_statistics.cpp
    storage/index/chunk_index_statistics.hpp
    storage/index/table_index_statistics.cpp
    storage/index/table_index_statistics.hpp
    storage/index/partial_hash/flat_map_iterator.cpp
    storage/index/partial_hash/flat_map_iterator.hpp
    storage/index/partial_hash/flat_map_iterator_impl.cpp
    storage/index/partial_hash/flat_map_iterator_impl.hpp
    storage/index/partial_hash/partial_hash_index.cpp
    storage/index/partial_hash/partial_hash_index.hpp
    storage/index/partial_hash/partial_hash_index_impl.cpp
    storage/index/partial_hash/partial_hash_index_impl.hpp
    storage/index/chunk_index_type.hpp
    storage/lqp_view.cpp
    storage/lqp_view.hpp
    storage/lz4_segment.cpp
    storage/lz4_segment.hpp
    storage/lz4_segment/lz4_encoder.hpp
    storage/lz4_segment/lz4_segment_iterable.hpp
    storage/materialize.hpp
    storage/mvcc_data.cpp
    storage/mvcc_data.hpp
    storage/pos_lists/abstract_pos_list.cpp
    storage/pos_lists/abstract_pos_list.hpp
    storage/pos_lists/entire_chunk_pos_list.cpp
    storage/pos_lists/entire_chunk_pos_list.hpp
    storage/pos_lists/row_id_pos_list.cpp
    storage/pos_lists/row_id_pos_list.hpp
    storage/prepared_plan.cpp
    storage/prepared_plan.hpp
    storage/reference_segment.cpp
    storage/reference_segment.hpp
    storage/reference_segment/reference_segment_iterable.hpp
    storage/resolve_encoded_segment_type.hpp
    storage/run_length_segment.cpp
    storage/run_length_segment.hpp
    storage/run_length_segment/run_length_encoder.hpp
    storage/run_length_segment/run_length_segment_iterable.hpp
    storage/segment_access_counter.cpp
    storage/segment_access_counter.hpp
    storage/segment_accessor.cpp
    storage/segment_accessor.hpp
    storage/segment_encoding_utils.cpp
    storage/segment_encoding_utils.hpp
    storage/segment_iterables.hpp
    storage/segment_iterables/abstract_segment_iterators.hpp
    storage/segment_iterables/any_segment_iterable.cpp
    storage/segment_iterables/any_segment_iterable.hpp
    storage/segment_iterables/any_segment_iterator.hpp
    storage/segment_iterables/create_iterable_from_attribute_vector.hpp
    storage/segment_iterables/segment_positions.hpp
    storage/segment_iterate.hpp
    storage/split_pos_list_by_chunk_id.cpp
    storage/split_pos_list_by_chunk_id.hpp
    storage/storage_manager.cpp
    storage/storage_manager.hpp
    storage/table.cpp
    storage/table.hpp
    storage/table_column_definition.cpp
    storage/table_column_definition.hpp
    storage/value_segment.cpp
    storage/value_segment.hpp
    storage/value_segment/null_value_vector_iterable.hpp
    storage/value_segment/value_segment_iterable.hpp
    storage/vector_compression/base_compressed_vector.hpp
    storage/vector_compression/base_vector_compressor.hpp
    storage/vector_compression/base_vector_decompressor.hpp
    storage/vector_compression/compressed_vector_type.cpp
    storage/vector_compression/compressed_vector_type.hpp
    storage/vector_compression/fixed_width_integer/fixed_width_integer_compressor.cpp
    storage/vector_compression/fixed_width_integer/fixed_width_integer_compressor.hpp
    storage/vector_compression/fixed_width_integer/fixed_width_integer_decompressor.hpp
    storage/vector_compression/fixed_width_integer/fixed_width_integer_utils.hpp
    storage/vector_compression/fixed_width_integer/fixed_width_integer_vector.hpp
    storage/vector_compression/resolve_compressed_vector_type.hpp
    storage/vector_compression/bitpacking/bitpacking_compressor.cpp
    storage/vector_compression/bitpacking/bitpacking_compressor.hpp
    storage/vector_compression/bitpacking/bitpacking_iterator.hpp
    storage/vector_compression/bitpacking/bitpacking_decompressor.hpp
    storage/vector_compression/bitpacking/bitpacking_vector.hpp
    storage/vector_compression/bitpacking/bitpacking_vector.cpp
    storage/vector_compression/bitpacking/bitpacking_vector_type.hpp
    storage/vector_compression/vector_compression.cpp
    storage/vector_compression/vector_compression.hpp
    strong_typedef.hpp
    tasks/chunk_compression_task.cpp
    tasks/chunk_compression_task.hpp
    type_comparison.hpp
    types.cpp
    types.hpp
    uid_allocator.hpp
    utils/abstract_plugin.cpp
    utils/abstract_plugin.hpp
    utils/aligned_size.hpp
    utils/assert.hpp
    utils/atomic_max.hpp
    utils/check_table_equal.cpp
    utils/check_table_equal.hpp
    utils/copyable_atomic.hpp
    utils/date_time_utils.cpp
    utils/date_time_utils.hpp
    utils/enum_constant.hpp
    utils/format_bytes.cpp
    utils/format_bytes.hpp
    utils/format_duration.cpp
    utils/format_duration.hpp
    utils/invalid_input_exception.hpp
    utils/list_directory.cpp
    utils/list_directory.hpp
    utils/load_table.cpp
    utils/load_table.hpp
    utils/log_manager.cpp
    utils/log_manager.hpp
    utils/lossless_predicate_cast.cpp
    utils/lossless_predicate_cast.hpp
    utils/make_bimap.hpp
    utils/map_prunable_subquery_predicates.hpp
    utils/meta_table_manager.cpp
    utils/meta_table_manager.hpp
    utils/meta_tables/abstract_meta_table.cpp
    utils/meta_tables/abstract_meta_table.hpp
    utils/meta_tables/meta_chunk_sort_orders_table.cpp
    utils/meta_tables/meta_chunk_sort_orders_table.hpp
    utils/meta_tables/meta_chunks_table.cpp
    utils/meta_tables/meta_chunks_table.hpp
    utils/meta_tables/meta_columns_table.cpp
    utils/meta_tables/meta_columns_table.hpp
    utils/meta_tables/meta_exec_table.cpp
    utils/meta_tables/meta_exec_table.hpp
    utils/meta_tables/meta_log_table.cpp
    utils/meta_tables/meta_log_table.hpp
    utils/meta_tables/meta_plugins_table.cpp
    utils/meta_tables/meta_plugins_table.hpp
    utils/meta_tables/meta_segments_accurate_table.cpp
    utils/meta_tables/meta_segments_accurate_table.hpp
    utils/meta_tables/meta_segments_table.cpp
    utils/meta_tables/meta_segments_table.hpp
    utils/meta_tables/meta_settings_table.cpp
    utils/meta_tables/meta_settings_table.hpp
    utils/meta_tables/meta_system_information_table.cpp
    utils/meta_tables/meta_system_information_table.hpp
    utils/meta_tables/meta_system_utilization_table.cpp
    utils/meta_tables/meta_system_utilization_table.hpp
    utils/meta_tables/meta_tables_table.cpp
    utils/meta_tables/meta_tables_table.hpp
    utils/meta_tables/segment_meta_data.cpp
    utils/meta_tables/segment_meta_data.hpp
    utils/pausable_loop_thread.cpp
    utils/pausable_loop_thread.hpp
    utils/performance_warning.cpp
    utils/performance_warning.hpp
    utils/plugin_manager.cpp
    utils/plugin_manager.hpp
    utils/print_utils.cpp
    utils/print_utils.hpp
    utils/pruning_utils.cpp
    utils/pruning_utils.hpp
    utils/settings/abstract_setting.cpp
    utils/settings/abstract_setting.hpp
    utils/settings_manager.cpp
    utils/settings_manager.hpp
    utils/singleton.hpp
    utils/size_estimation_utils.hpp
    utils/sqlite_add_indices.cpp
    utils/sqlite_add_indices.hpp
    utils/sqlite_wrapper.cpp
    utils/sqlite_wrapper.hpp
    utils/string_utils.cpp
    utils/string_utils.hpp
    utils/template_type.hpp
    utils/timer.cpp
    utils/timer.hpp
    visualization/abstract_visualizer.hpp
    visualization/join_graph_visualizer.cpp
    visualization/join_graph_visualizer.hpp
    visualization/lqp_visualizer.cpp
    visualization/lqp_visualizer.hpp
    visualization/pqp_visualizer.cpp
    visualization/pqp_visualizer.hpp
    visualization/viz_record_layout.cpp
    visualization/viz_record_layout.hpp
    ${CMAKE_BINARY_DIR}/version.hpp
)

set(
    LIBRARIES
    compact_vector
    lz4
    magic_enum
    sqlparser
    uninitialized_vector
    libzstd_static
    ${CMAKE_DL_LIBS}
    # Additional libraries added with target_link_libraries_system below
)

if (NOT APPLE)
    set(LIBRARIES ${LIBRARIES} atomic pthread)
endif()

if (${ENABLE_NUMA_SUPPORT})
    set(LIBRARIES ${LIBRARIES} ${NUMA_LIBRARY})
endif()

set(ERASE_SEGMENT_TYPES "" CACHE STRING "Erase iterators and accessors for these types (e.g., RunLength,FrameOfReference). Good for compile time, bad for run time (if these types end up being used)")
string(REPLACE "," ";" ERASE_SEGMENT_TYPES_LIST "${ERASE_SEGMENT_TYPES}")
string(TOUPPER "${ERASE_SEGMENT_TYPES_LIST}" ERASE_SEGMENT_TYPES_UPPER)
if (NOT "${ERASE_SEGMENT_TYPES_UPPER}" STREQUAL "")
    foreach(ERASE_SEGMENT_TYPE ${ERASE_SEGMENT_TYPES_UPPER})
        message(STATUS "Erasing iterators and accessors for: ${ERASE_SEGMENT_TYPE}")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DHYRISE_ERASE_${ERASE_SEGMENT_TYPE}")
    endforeach()
endif()

# hyrise_impl should not be directly used, as linking to it regularly excludes unused symbols. See hyrise below.
if(NOT ENABLE_COVERAGE AND NOT (APPLE AND (ENABLE_ADDR_UB_LEAK_SANITIZATION OR ENABLE_THREAD_SANITIZATION)))
    add_library(hyrise_impl SHARED ${SOURCES})
else()
    # Use a static build for coverage information - otherwise .cpp files are missing from the report.
    # Also, the sanitizer builds on Mac require static linkage.
    add_library(hyrise_impl STATIC ${SOURCES})
endif()

# Work around an stdlibc++ bug, see list_directory.hpp for details
set_source_files_properties(
    utils/list_directory.cpp
    PROPERTIES
    COMPILE_FLAGS -O0
    SKIP_PRECOMPILE_HEADERS TRUE
)

# -rdynamic tells the linker to export the library's symbols so that plugins can use them.
target_link_libraries(hyrise_impl PUBLIC ${LIBRARIES} -rdynamic)

if(NOT APPLE)
    # Do not use jemalloc on Mac as we cannot properly replace the allocator - see third_party/CMakeLists.txt for details.
    target_link_libraries(hyrise_impl PUBLIC custom_jemalloc -rdynamic)
    target_compile_definitions(hyrise_impl PUBLIC HYRISE_WITH_JEMALLOC)

    # Add the jemalloc headers to Hyrise so that we can use jemalloc-specific functions.
    # We need to use a path relative to CMAKE_CURRENT_BINARY_DIR instead of using CMAKE_BINARY_DIR because Hyrise might
    # not be the top-level CMake project (e.g., if it is used as a submodule in a Hyrise plugin repository).
    target_include_directories(hyrise_impl SYSTEM PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/../../third_party/jemalloc/include)
endif()

target_link_libraries_system(
    hyrise_impl

    ${BOOST_LIBS}
    concurrentqueue
    cxxopts
    nlohmann_json::nlohmann_json
    sparse_map
    SQLite::SQLite3
    tbb
)

if (NUMA_FOUND)
    target_link_libraries(hyrise_impl PUBLIC numa)
endif()

target_include_directories(hyrise_impl PUBLIC ${CMAKE_BINARY_DIR})

# Precompile the most expensive headers. Only add headers here if you know what you are doing, as this has the potential
# of negatively affecting the build time.
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
target_compile_options(hyrise_impl PUBLIC -Xclang -fno-pch-timestamp)
endif()
target_precompile_headers(hyrise_impl PRIVATE
  all_parameter_variant.hpp
  storage/create_iterable_from_segment.hpp
  storage/table.hpp
  types.hpp
)

# -fPIC generates position independent code which is necessary because plugins might access hyrise functions.
target_compile_options(hyrise_impl PRIVATE -fPIC)

# Dependency hyrise --> git_watcher needs to be added manually, apparently. Otherwise version.hpp, for reasons unknown
# isn't updated when HEAD changes

add_dependencies(hyrise_impl AlwaysCheckGit)

# This is a wrapper around hyrise_impl that forces the compiler to emit all symbols
add_library(hyrise INTERFACE)
# Forward all include directories from hyrise_impl
target_include_directories(hyrise INTERFACE $<TARGET_PROPERTY:hyrise_impl,INCLUDE_DIRECTORIES>)

# -force_load/--whole-archive are necessary to include all symbols so that dynamically loaded plugins can use them
if (APPLE)
    set_property(TARGET hyrise PROPERTY INTERFACE_LINK_LIBRARIES -force_load $<TARGET_FILE:hyrise_impl>)
else()
    set_property(TARGET hyrise PROPERTY INTERFACE_LINK_LIBRARIES -Wl,--whole-archive,$<TARGET_FILE:hyrise_impl>,--no-whole-archive)
endif()

# Link against all libraries used by hyrise_impl - do this after INTERFACE_LINK_LIBRARIES is initially set so that it is not overwritten
target_link_libraries(hyrise INTERFACE hyrise_impl)

# For convenience, to be able to launch, e.g., hyriseTest, directly from the build directory,
# add a link to the resources/ directory at the root of the build directory).
add_custom_target(resourceLink
        COMMAND ln -fs ${CMAKE_BINARY_DIR}/../resources ${CMAKE_BINARY_DIR}/
)

add_dependencies(hyrise resourceLink)
