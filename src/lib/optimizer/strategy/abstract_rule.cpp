#include "abstract_rule.hpp"

#include <algorithm>
#include <memory>

#include "expression/lqp_subquery_expression.hpp"  // IWYU pragma: keep
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace hyrise {

IsCacheable operator&(IsCacheable lhs, IsCacheable rhs) {
  return static_cast<IsCacheable>(static_cast<bool>(lhs) & static_cast<bool>(rhs));
}

IsCacheable& operator&=(IsCacheable& lhs, const IsCacheable rhs) {
  return lhs = lhs & rhs;
}

IsCacheable AbstractRule::apply_to_plan(const std::shared_ptr<LogicalPlanRootNode>& lqp_root) const {
  // (1) Optimize root LQP.
  auto cacheable = _apply_to_plan_without_subqueries(lqp_root);

  // (2) Optimize distinct subquery LQPs, one-by-one.
  const auto subquery_expressions_by_lqp = collect_lqp_subquery_expressions_by_lqp(lqp_root);
  for (const auto& [lqp, subquery_expressions] : subquery_expressions_by_lqp) {
    if (std::all_of(subquery_expressions.cbegin(), subquery_expressions.cend(), [](const auto& subquery_expression) {
          return subquery_expression.expired();
        })) {
      continue;
    }

    // (2.1) Optimize subplan.
    const auto local_lqp_root = LogicalPlanRootNode::make(lqp);
    cacheable &= _apply_to_plan_without_subqueries(local_lqp_root);

    // (2.2) Assign optimized subplan to all corresponding SubqueryExpressions.
    for (const auto& subquery_expression : subquery_expressions) {
      subquery_expression.lock()->lqp = local_lqp_root->left_input();
    }

    // (2.3) Untie the root node before it goes out of scope so that the outputs of the LQP remain correct.
    local_lqp_root->set_left_input(nullptr);
  }

  return cacheable;
}

}  // namespace hyrise
