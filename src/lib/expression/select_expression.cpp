#include "select_expression.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

SelectExpression::SelectExpression(const std::shared_ptr<AbstractLQPNode>& lqp): AbstractExpression(ExpressionType::Select), lqp(lqp) {

}

bool SelectExpression::deep_equals(const AbstractExpression& expression) const {
  if (type != expression.type) return false;

  const auto& select_expression = static_cast<const SelectExpression&>(expression);
  return !lqp->find_first_subplan_mismatch(select_expression.lqp).has_value();
}

std::shared_ptr<AbstractExpression> SelectExpression::deep_copy() const {
  return std::make_shared<SelectExpression>(lqp->deep_copy());
}

std::shared_ptr<AbstractExpression> SelectExpression::deep_resolve_column_expressions() {
  return shared_from_this();
}

}  // namespace opossum
