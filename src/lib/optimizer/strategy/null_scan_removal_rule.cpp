#include "null_scan_removal_rule.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
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

void NullScanRemovalRule::apply_to_plan(const std::shared_ptr<LogicalPlanRootNode>& root) const {
  Assert(root->type == LQPNodeType::Root, "NullScanRemovalRule needs root to hold onto");

  std::vector<std::shared_ptr<AbstractLQPNode>> nodes_to_remove;

  // To determine if the rule applies to a node it must meet all of the following conditions:
  // 1. The node must be of type Predicate
  // 2. The predicate must be a null expression
  // 3. The predicate condition must be is not null
  // 4. The predicate operand needs to be an LQP Column expression
  // 5. The original node of the LQP Column expression needs to be a storage table node
  // 6. The column (referenced by the LQP Column expression) is not nullable
  // All nodes where the conditions apply are removed from the LQP.
  auto visitor = [&](const auto& node) {
    // Checks condition 1
    if (node->type != LQPNodeType::Predicate) {
      return LQPVisitation::VisitInputs;
    }

    // Checks for condition 2
    const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);
    const auto predicate = predicate_node->predicate();
    const auto is_null_expression = std::dynamic_pointer_cast<IsNullExpression>(predicate);
    if (!is_null_expression) {
      return LQPVisitation::VisitInputs;
    }

    // Checks for condition 3
    if (is_null_expression->predicate_condition == PredicateCondition::IsNull) {
      return LQPVisitation::VisitInputs;
    }

    // Checks for condition 4
    const auto& column = std::dynamic_pointer_cast<LQPColumnExpression>(is_null_expression->operand());
    if (!column) {
      return LQPVisitation::VisitInputs;
    }

    // Checks for condition 5
    const auto& stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(column->original_node.lock());
    if (!stored_table_node) {
      return LQPVisitation::VisitInputs;
    }

    // Checks for condition 6
    const auto& table = Hyrise::get().storage_manager.get_table(stored_table_node->table_name);
    const auto original_column_id = column->original_column_id;
    const auto table_column_definition = table->column_definitions()[original_column_id];
    if (table_column_definition.nullable == true) {
      return LQPVisitation::VisitInputs;
    }

    nodes_to_remove.push_back(node);
    return LQPVisitation::VisitInputs;
  };

  visit_lqp(root, visitor);

  _remove_nodes(nodes_to_remove);
}

void NullScanRemovalRule::_remove_nodes(const std::vector<std::shared_ptr<AbstractLQPNode>>& nodes) {
  for (const auto& node : nodes) {
    lqp_remove_node(node);
  }
}

void NullScanRemovalRule::_apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  Fail("Did not expect this function to be called.");
}

}  // namespace hyrise
