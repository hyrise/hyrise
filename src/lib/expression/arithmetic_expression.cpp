#include "arithmetic_expression.hpp"

namespace opossum {

ArithmeticExpression::ArithmeticExpression(const ArithmeticOperator arithmetic_operator,
                                           const std::shared_ptr<AbstractExpression>& left_operand,
                                           const std::shared_ptr<AbstractExpression>& right_operand):
  AbstractExpression(ExpressionType::Arithmetic), arithmetic_operator(arithmetic_operator), left_operand(left_operand), right_operand(right_operand) {

}

std::shared_ptr<AbstractExpression> ArithmeticExpression::deep_copy() const {
  return std::make_shared<ArithmeticExpression>(arithmetic_operator, left_operand->deep_copy(), right_operand->deep_copy());
}

std::shared_ptr<AbstractExpression> ArithmeticExpression::resolve_expression_columns() const {
  left_operand = left_operand->resolve_expression_columns();
  right_operand = right_operand->resolve_expression_columns();
  return shared_from_this();
}

}  // namespace opossum
