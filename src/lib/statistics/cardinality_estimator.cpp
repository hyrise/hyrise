#include "cardinality_estimator.hpp"

#include "chunk_statistics2.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "resolve_type.hpp"
#include "segment_statistics2.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "table_statistics2.hpp"
#include "utils/assert.hpp"

namespace opossum {

Cardinality CardinalityEstimator::estimate_cardinality(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  return estimate_statistics(lqp)->row_count();
}

std::shared_ptr<TableStatistics2> CardinalityEstimator::estimate_statistics(
    const std::shared_ptr<AbstractLQPNode>& lqp) const {
  if (const auto mock_node = std::dynamic_pointer_cast<MockNode>(lqp)) {
    Assert(mock_node->table_statistics2(), "");
    return mock_node->table_statistics2();
  }

  if (const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(lqp)) {
    const auto table = StorageManager::get().get_table(stored_table_node->table_name);
    Assert(table->table_statistics2(), "");
    return table->table_statistics2();
  }

  if (const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(lqp)) {
    const auto input_table_statistics = estimate_statistics(lqp->left_input());
    const auto output_table_statistics = std::make_shared<TableStatistics2>();

    const auto operator_scan_predicates =
        OperatorScanPredicate::from_expression(*predicate_node->predicate, *predicate_node);
    Assert(operator_scan_predicates, "NYI");

    for (auto chunk_id = ChunkID{0}; chunk_id < input_table_statistics->chunk_statistics.size(); ++chunk_id) {
      const auto input_chunk_statistics = input_table_statistics->chunk_statistics[chunk_id];
      auto output_chunk_statistics = input_chunk_statistics;

      for (const auto& operator_scan_predicate : *operator_scan_predicates) {
        const auto predicate_input_chunk_statistics = output_chunk_statistics;
        const auto predicate_output_chunk_statistics = std::make_shared<ChunkStatistics2>();
        predicate_output_chunk_statistics->segment_statistics.resize(predicate_input_chunk_statistics->segment_statistics.size());

        /**
         * Manipulate statistics of column that we scan on
         */
        const auto base_segment_statistics =
          output_chunk_statistics->segment_statistics.at(operator_scan_predicate.column_id);

        const auto data_type = predicate_node->column_expressions().at(operator_scan_predicate.column_id)->data_type();

        resolve_data_type(data_type, [&](const auto data_type_t) {
          using SegmentDataType = typename decltype(data_type_t)::type;

          const auto segment_statistics =
              std::static_pointer_cast<SegmentStatistics2<SegmentDataType>>(base_segment_statistics);
          Assert(segment_statistics->equal_distinct_count_histogram, "");

          const auto sliced_statistics_object =
              segment_statistics->equal_distinct_count_histogram->slice_with_predicate(
                  operator_scan_predicate.predicate_condition, operator_scan_predicate.value,
                  operator_scan_predicate.value2);

          const auto output_segment_statistics = std::make_shared<SegmentStatistics2>();
          output_segment_statistics->equal_distinct_count_histogram =

          predicate_output_chunk_statistics->segment_statistics[operator_scan_predicate.column_id] = ;
        });

        /**
         * Manipulate statistics of all columns that we DIDN'T scan on with this predicate
         */
        for (auto column_id = ColumnID{0};
             column_id < predicate_input_chunk_statistics->segment_statistics.size();
             ++column_id) {
          if (column_id == operator_scan_predicate.column_id) continue;

          predicate_output_chunk_statistics->segment_statistics[column_id] =  ...
        }

        /**
         * Adjust ChunkStatistics row_count
         */
        predicate_output_chunk_statistics->row_count = predicate_input_chunk_statistics->row_count * predicate_chunk_selectivity;

        output_chunk_statistics = predicate_output_chunk_statistics;
      }

      output_table_statistics->chunk_statistics.emplace_back(output_chunk_statistics);
    }

    return output_table_statistics;
  }

  Fail("NYI");
}

}  // namespace opossum
