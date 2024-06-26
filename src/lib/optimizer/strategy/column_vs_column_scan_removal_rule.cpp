#include "column_vs_column_scan_removal_rule.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <queue>

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

void remove_scans_recursively(const std::shared_ptr<AbstractLQPNode>& node,
                                         std::unordered_set<std::shared_ptr<AbstractLQPNode>>& visited_nodes,
                                         ExpressionUnorderedMap<ExpressionUnorderedSet>& equivalencies) {
  if (!node || !visited_nodes.emplace(node).second) {
    return;
  }

  remove_scans_recursively(node->left_input(), visited_nodes, equivalencies);
  remove_scans_recursively(node->right_input(), visited_nodes, equivalencies);

  const auto node_type = node->type;
  if (node_type != LQPNodeType::Join && node_type != LQPNodeType::Predicate) {
    return;
  }

  if (node->type == LQPNodeType::Join) {
    for (auto& expression : node->node_expressions) {
      const auto& predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(expression);
      Assert(predicate, "Join predicate must be binary predicate.");
      if (predicate->predicate_condition == PredicateCondition::Equals) {
        equivalencies[predicate->left_operand()].emplace(predicate->right_operand());
        equivalencies[predicate->right_operand()].emplace(predicate->left_operand());
      }
    }
    return;
  }

  Assert(node->node_expressions.size() == 1, "PredicateNode should have exactly one predicate.");
  const auto& binary_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(node->node_expressions[0]);
  if (!binary_predicate) {
    return;
  }

  const auto& left_operand = binary_predicate->left_operand();
  const auto& right_operand = binary_predicate->right_operand();

  if (left_operand->type != ExpressionType::LQPColumn || right_operand->type != ExpressionType::LQPColumn) {
    return;
  }

  // We do not have to search if the columns are no join keys.
  if (!equivalencies.contains(left_operand) || !equivalencies.contains(right_operand)) {
    return;
  }

  // Try if the predicate columns are equivalent, i.e, they are from join keys (or transitive keys). We perform a BFS
  // and see if we can reach the right operand using the equivalencies. No need to use an ExpressionUnorderedSet to
  // track the visited expressions.
  auto visited_expressions = std::unordered_set<std::shared_ptr<AbstractExpression>>{};
  auto expressions_to_visit = std::queue<std::shared_ptr<AbstractExpression>>{};
  expressions_to_visit.push(left_operand);

  while (!expressions_to_visit.empty()) {
    const auto& candidate_left_expression = expressions_to_visit.front();
    expressions_to_visit.pop();
    visited_expressions.emplace(candidate_left_expression);

    for (const auto& candidate_right_expression : equivalencies.at(candidate_left_expression)) {
      // If we can reach the right operand, remove the PredicateNode.
      if (*candidate_right_expression == *right_operand) {
        lqp_remove_node(node);
        return;
      }

      if (!visited_expressions.contains(candidate_right_expression)) {
        expressions_to_visit.push(candidate_right_expression);
      }
    }
  }
}

}  // namespace

namespace hyrise {

std::string ColumnVsColumnScanRemovalRule::name() const {
  static const auto name = std::string{"ColumnVsColumnScanRemovalRule"};
  return name;
}

void ColumnVsColumnScanRemovalRule::_apply_to_plan_without_subqueries(
    const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  // Cache cost of all intermediate operators and joins with reordered predicates.

  // In theory, the order of join predicates does not make a difference for cardinality estimation, but in fact we are
  // only using the first (ideally least selective) predicate. Thus, we must actually reorder the predicates bottom-up
  // to get the best estimations based on possibly reordered multipe-predicate joins below.
  auto visited_nodes = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};
  auto equivalencies = ExpressionUnorderedMap<ExpressionUnorderedSet>{};
  remove_scans_recursively(lqp_root, visited_nodes, equivalencies);
}

}  // namespace hyrise
