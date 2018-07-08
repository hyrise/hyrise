#include "abstract_rule.hpp"

#include <memory>

#include "expression/expression_utils.hpp"
#include "expression/lqp_select_expression.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

bool AbstractRule::_apply_recursively(std::shared_ptr<AbstractLQPNode> node) {
  auto inputs_changed = false;

  // Apply this rule recursively
  if (node->left_input()) {
    inputs_changed |= apply_to(node->left_input());
  }
  if (node->right_input()) {
    inputs_changed |= apply_to(node->right_input());
  }

  // Apply the rule to all Subqueries in
  for (const auto& expression : node->node_expressions()) {
    visit_expression(expression, [&](const auto& sub_expression) {
      const auto lqp_select_expression = std::dynamic_pointer_cast<LQPSelectExpression>(sub_expression);
      if (!lqp_select_expression) return true;

      inputs_changed |= apply_to(lqp_select_expression->lqp);

      return false;
    });
  }

  return inputs_changed;
}

}  // namespace opossum
