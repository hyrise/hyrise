#include "arithmetic_expression.hpp"

#include <sstream>

namespace opossum {

ArithmeticExpression::ArithmeticExpression(const ArithmeticOperator arithmetic_operator,
                                           const std::shared_ptr<AbstractExpression>& left_operand,
                                           const std::shared_ptr<AbstractExpression>& right_operand):
AbstractExpression(ExpressionType::Arithmetic, {left_operand, right_operand}), arithmetic_operator(arithmetic_operator) {}

const std::shared_ptr<AbstractExpression>& ArithmeticExpression::left_operand() const {
  return arguments[0];
}

const std::shared_ptr<AbstractExpression>& ArithmeticExpression::right_operand() const {
  return arguments[1];
}

std::shared_ptr<AbstractExpression> ArithmeticExpression::deep_copy() const {
  return std::make_shared<ArithmeticExpression>(arithmetic_operator, left_operand()->deep_copy(), left_operand()->deep_copy());
}

std::string ArithmeticExpression::description() const {
  std::stringstream stream;

  Fail("Todo");

  return stream.str();
}

bool ArithmeticExpression::_shallow_equals(const AbstractExpression& expression) const {
  return arithmetic_operator == static_cast<const ArithmeticExpression&>(expression).arithmetic_operator;
}

}  // namespace opossum
