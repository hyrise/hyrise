#include "null_scan_removal_rule.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "statistics/cardinality_estimation_cache.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"

namespace hyrise {

std::string NullScanRemovalRule::name() const {
  static const auto name = std::string{"NullScanRemovalRule"};
  return name;
}

void NullScanRemovalRule::_apply_to_plan_without_subqueries(
    const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  // To determine if the rule applies to a node it must meet all of the following conditions:
  // 1. The node must be of type Predicate.
  // 2. The predicate must be an IsNullExpression with predicate condition IsNotNull.
  // 3. The predicate operand must not be nullable.
  // All nodes where the conditions apply are removed from the LQP.
  auto nodes_to_remove = std::vector<std::shared_ptr<AbstractLQPNode>>{};

  visit_lqp(lqp_root, [&](const auto& node) {
    // Checks condition 1.
    if (node->type != LQPNodeType::Predicate) {
      return LQPVisitation::VisitInputs;
    }

    // Checks for condition 2.
    const auto& predicate = static_cast<const PredicateNode&>(*node).predicate();
    const auto is_null_expression = std::dynamic_pointer_cast<IsNullExpression>(predicate);

    if (!is_null_expression || is_null_expression->predicate_condition != PredicateCondition::IsNotNull) {
      return LQPVisitation::VisitInputs;
    }

    // Checks for condition 3.
    const auto column_id = node->get_column_id(*is_null_expression->operand());
    if (node->is_column_nullable(column_id)) {
      return LQPVisitation::VisitInputs;
    }

    nodes_to_remove.emplace_back(node);
    return LQPVisitation::VisitInputs;
  });

  // Untie selected nodes from LQP.
  for (const auto& node : nodes_to_remove) {
    lqp_remove_node(node);
  }
}

}  // namespace hyrise
