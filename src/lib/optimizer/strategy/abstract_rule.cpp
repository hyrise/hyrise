#include "abstract_rule.hpp"

#include <memory>

#include "expression/expression_utils.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"

namespace hyrise {

void AbstractRule::apply_to_plan(const std::shared_ptr<LogicalPlanRootNode>& lqp_root) const {
  // (1) Optimize root LQP
  _apply_to_plan_without_subqueries(lqp_root);

  // (2) Optimize distinct subquery LQPs, one-by-one.
  auto subquery_expressions_by_lqp = collect_lqp_subquery_expressions_by_lqp(lqp_root);
  for (const auto& [lqp, subquery_expressions] : subquery_expressions_by_lqp) {
    if (std::all_of(subquery_expressions.cbegin(), subquery_expressions.cend(),
                    [](auto subquery_expression) { return subquery_expression.expired(); })) {
      continue;
    }

    // (2.1) Optimize subplan
    const auto local_lqp_root = LogicalPlanRootNode::make(lqp);
    _apply_to_plan_without_subqueries(local_lqp_root);

    // (2.2) Assign optimized subplan to all corresponding SubqueryExpressions
    for (const auto& subquery_expression : subquery_expressions) {
      subquery_expression.lock()->lqp = local_lqp_root->left_input();
    }

    // (2.3) Untie the root node before it goes out of scope so that the outputs of the LQP remain correct.
    local_lqp_root->set_left_input(nullptr);
  }
}

}  // namespace hyrise
