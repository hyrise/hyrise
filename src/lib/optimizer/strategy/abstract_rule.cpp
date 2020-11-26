#include "abstract_rule.hpp"

#include <memory>

#include "expression/expression_utils.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"

namespace opossum {


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
