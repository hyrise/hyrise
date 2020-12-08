#include "null_scan_removal_rule.hpp"

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "cost_estimation/abstract_cost_estimator.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "statistics/cardinality_estimation_cache.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

void NullScanRemovalRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  Assert(root->type == LQPNodeType::Root, "NullScanRemovalRule needs root to hold onto");
  _remove_nodes(_nodes_to_remove(root));
}

std::vector<std::shared_ptr<AbstractLQPNode>> NullScanRemovalRule::_nodes_to_remove(
    const std::shared_ptr<AbstractLQPNode>& root) const {
  std::vector<std::shared_ptr<AbstractLQPNode>> nodes_to_remove;

  auto visiter = [&](const auto& node) {
    if (node->type != LQPNodeType::Predicate) return LQPVisitation::VisitInputs;

    const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);
    const auto predicate = predicate_node->predicate();

    if (const auto is_null_expression = std::dynamic_pointer_cast<IsNullExpression>(predicate)) {
      if (is_null_expression->predicate_condition == PredicateCondition::IsNull) return LQPVisitation::VisitInputs;

      const auto& column = std::dynamic_pointer_cast<LQPColumnExpression>(is_null_expression->operand());
      if (!column) return LQPVisitation::VisitInputs;

      const auto& stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(column->original_node.lock());
      if (!stored_table_node) return LQPVisitation::VisitInputs;

      const auto& table = Hyrise::get().storage_manager.get_table(stored_table_node->table_name);

      const auto original_column_id = column->original_column_id;

      const auto table_column_definition = table->column_definitions()[original_column_id];

      if (table_column_definition.nullable == false) {
        nodes_to_remove.push_back(node);
      }
    }
    return LQPVisitation::VisitInputs;
  };
  visit_lqp(root, visiter);
  return nodes_to_remove;
}

void NullScanRemovalRule::_remove_nodes(const std::vector<std::shared_ptr<AbstractLQPNode>>& nodes) const {
  for (const auto& node : nodes) {
    lqp_remove_node(node);
  }
}

}  // namespace opossum
