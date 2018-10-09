#include "cardinality_estimator.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "utils/assert.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "table_statistics2.hpp"

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

  Assert(lqp->left_input(), "");
  Assert(!lqp->right_input(), "NYI");

  const auto input_statistics = estimate_statistics(lqp->left_input());


}

}  // namespace opossum
