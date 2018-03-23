#include "logical_expression.hpp"

#include "utils/assert.hpp"

namespace opossum {

LogicalExpression::LogicalExpression(const LogicalOperator logical_operator, const std::shared_ptr<AbstractExpression>& left_operand, const std::shared_ptr<AbstractExpression>& right_operand):
AbstractExpression(ExpressionType::Logical), logical_operator(logical_operator), left_operand(left_operand), right_operand(right_operand) {}

bool LogicalExpression::deep_equals(const AbstractExpression& expression) const {
  if (type != expression.type) return false;

  const auto& logical_expression = static_cast<const LogicalExpression&>(expression);
  if (logical_operator != logical_expression.logical_operator) return false;
  return deep_equals_expressions({left_operand, right_operand}, {logical_expression.left_operand, logical_expression.right_operand});
}

std::shared_ptr<AbstractExpression> LogicalExpression::deep_copy() const {
  return std::make_shared<LogicalExpression>(logical_operator, left_operand->deep_copy(), right_operand->deep_copy());
}

std::shared_ptr<AbstractExpression> LogicalExpression::deep_resolve_column_expressions() {
  left_operand = left_operand->deep_resolve_column_expressions();
  right_operand = right_operand->deep_resolve_column_expressions();

  return shared_from_this();
}

}  // namespace opossum
