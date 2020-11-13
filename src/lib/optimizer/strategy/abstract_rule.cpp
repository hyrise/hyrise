#include "abstract_rule.hpp"

#include <memory>

#include "expression/expression_utils.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"

namespace opossum {

/**
 * IMPORTANT NOTES ON OPTIMIZING SUBQUERY LQPS
 *
 * Multiple Expressions in different nodes might reference the same LQP. Most commonly this will be the case for a
 * ProjectionNode computing a subquery and a subsequent PredicateNode filtering based on it.
 * We do not WANT to optimize the LQP twice (optimization takes time after all) and we CANNOT optimize it twice, since,
 * e.g., a non-deterministic rule, could produce two different LQPs while optimizing and then the SubqueryExpression
 * in the PredicateNode couldn't be resolved to a column anymore. There are more subtle ways LQPs might break in this
 * scenario, and frankly, this is one of the weak links in the expression system...
 *
 * ...long story short:
 * !!!
 * EACH UNIQUE SUB-LQP IS ONLY OPTIMIZED ONCE, EVEN IF IT OCCURS IN DIFFERENT NODES/EXPRESSIONS.
 * !!!
 */
void AbstractRule::apply(const std::shared_ptr<AbstractLQPNode>& root_node) const {
  // (1) Optimize root LQP
  _apply_to(root_node);

  // (2) Optimize distinct subquery LQPs, one-by-one.
  auto subquery_expressions_by_lqp = SubqueryExpressionsByLQP{};
  collect_subquery_expressions_by_lqp(subquery_expressions_by_lqp, root_node);
  for (const auto& [lqp, subquery_expressions] : subquery_expressions_by_lqp) {
    // (2.1) Prepare
    const auto local_root_node = LogicalPlanRootNode::make(lqp);
    // (2.2) Optimize subquery LQP
    _apply_to(local_root_node);
    // (2.3) Assign optimized LQP to all corresponding SubqueryExpressions
    for (const auto& subquery_expression : subquery_expressions) {
      subquery_expression->lqp = local_root_node->left_input();
    }
    local_root_node->set_left_input(nullptr);  // TODO remove?
  }
}

void AbstractRule::_apply_to_inputs(std::shared_ptr<AbstractLQPNode> node) const {  // NOLINT
  if (node->left_input()) _apply_to(node->left_input());
  if (node->right_input()) _apply_to(node->right_input());
}

void collect_subquery_expressions_by_lqp(SubqueryExpressionsByLQP& subquery_expressions_by_lqp,
                                         const std::shared_ptr<AbstractLQPNode>& node,
                                         std::unordered_set<std::shared_ptr<AbstractLQPNode>>& visited_nodes) {
  if (!node) return;
  if (!visited_nodes.emplace(node).second) return;

  for (const auto& expression : node->node_expressions) {
    visit_expression(expression, [&](const auto& sub_expression) {
      const auto subquery_expression = std::dynamic_pointer_cast<LQPSubqueryExpression>(sub_expression);
      if (!subquery_expression) return ExpressionVisitation::VisitArguments;

      for (auto& [lqp, subquery_expressions] : subquery_expressions_by_lqp) {
        if (*lqp == *subquery_expression->lqp) {
          subquery_expressions.emplace_back(subquery_expression);
          return ExpressionVisitation::DoNotVisitArguments;
        }
      }
      subquery_expressions_by_lqp.emplace_back(subquery_expression->lqp, std::vector{subquery_expression});

      return ExpressionVisitation::DoNotVisitArguments;
    });
  }

  collect_subquery_expressions_by_lqp(subquery_expressions_by_lqp, node->left_input(), visited_nodes);
  collect_subquery_expressions_by_lqp(subquery_expressions_by_lqp, node->right_input(), visited_nodes);
}

}  // namespace opossum
