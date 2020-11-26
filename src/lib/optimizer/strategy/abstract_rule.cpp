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
void AbstractRule::apply(const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  // (1) Optimize root LQP
  _apply_to(lqp_root);

  // (2) Optimize distinct subquery LQPs, one-by-one.
  auto subquery_expressions_by_lqp = collect_subquery_expressions_by_lqp(lqp_root);
  for (const auto& [lqp, subquery_expressions] : subquery_expressions_by_lqp) {
    // (2.1) Prepare
    const auto local_lqp_root = LogicalPlanRootNode::make(lqp);
    // (2.2) Optimize subquery LQP
    _apply_to(local_lqp_root);
    // (2.3) Assign optimized LQP to all corresponding SubqueryExpressions
    for (const auto& subquery_expression : subquery_expressions) {
      subquery_expression->lqp = local_lqp_root->left_input();
    }
    // (2.4) Untie the root node before it goes out of scope so that the outputs of the LQP remain correct.
    local_lqp_root->set_left_input(nullptr);
  }
}

void AbstractRule::_apply_to_inputs(std::shared_ptr<AbstractLQPNode> node) const {  // NOLINT
  if (node->left_input()) _apply_to(node->left_input());
  if (node->right_input()) _apply_to(node->right_input());
}

}  // namespace opossum
