#include "cardinality_estimator.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "utils/assert.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "chunk_statistics2.hpp"
#include "segment_statistics2.hpp"
#include "table_statistics2.hpp"
#include "resolve_type.hpp"

namespace opossum {

Cardinality CardinalityEstimator::estimate_cardinality(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  return estimate_statistics(lqp)->row_count();
}

std::shared_ptr<TableStatistics2> CardinalityEstimator::estimate_statistics(const std::shared_ptr<AbstractLQPNode>& lqp) const {
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

    const auto operator_scan_predicates = OperatorScanPredicate::from_expression(*predicate_node->predicate, *predicate_node);
    Assert(operator_scan_predicates, "NYI");

    for (auto chunk_id = ChunkID{0}; chunk_id < input_table_statistics->chunk_statistics.size(); ++chunk_id) {
      const auto input_chunk_statistics = input_table_statistics->chunk_statistics[chunk_id]

      for (const auto& operator_scan_predicate : *operator_scan_predicates) {
        const auto base_segment_statistics = input_chunk_statistics->segment_statistics.at(operator_scan_predicate.column_id);

        const auto data_type = predicate_node->column_expressions().at(operator_scan_predicate.column_id)->data_type();

        resolve_data_type(data_type, [&](const auto data_type_t) {
          using SegmentDataType = typename decltype(data_type_t)::type;

          const auto segment_statistics = std::static_pointer_cast<SegmentStatistics2<SegmentDataType>>(base_segment_statistics);
          Assert(segment_statistics->equal_distinct_count_histogram, "");

          const auto sliced_statistics_object = segment_statistics->equal_distinct_count_histogram->slice_with_predicate(
            operator_scan_predicate.predicate_condition,
            operator_scan_predicate.value,
            operator_scan_predicate.value2
          );
        });
      }
    }

    return output_table_statistics;
  }




  Fail("NYI")
}

}  // namespace opossum
