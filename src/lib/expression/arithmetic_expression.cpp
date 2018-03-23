#include "arithmetic_expression.hpp"

#include "utils/assert.hpp"

namespace opossum {

ArithmeticExpression::ArithmeticExpression(const ArithmeticOperator arithmetic_operator, const std::shared_ptr<AbstractExpression>& left_operand, const std::shared_ptr<AbstractExpression>& right_operand):
AbstractExpression(ExpressionType::Arithmetic), arithmetic_operator(arithmetic_operator), left_operand(left_operand), right_operand(right_operand) {}

bool ArithmeticExpression::deep_equals(const AbstractExpression& expression) const {
  if (type != expression.type) return false;

  const auto& arithmetic_expression = static_cast<const ArithmeticExpression&>(expression);
  if (arithmetic_operator != arithmetic_expression.arithmetic_operator) return false;
  return deep_equals_expressions({left_operand, right_operand}, {arithmetic_expression.left_operand, arithmetic_expression.right_operand});
}

std::shared_ptr<AbstractExpression> ArithmeticExpression::deep_copy() const {
  return std::make_shared<ArithmeticExpression>(arithmetic_operator, left_operand->deep_copy(), right_operand->deep_copy());
}

std::shared_ptr<AbstractExpression> ArithmeticExpression::deep_resolve_column_expressions() {
  left_operand = left_operand->deep_resolve_column_expressions();
  right_operand = right_operand->deep_resolve_column_expressions();

  return shared_from_this();
}

}  // namespace opossum
