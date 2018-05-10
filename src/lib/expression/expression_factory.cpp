#include "expression_factory.hpp"

namespace opossum {

namespace expression_factory  {

std::shared_ptr<AbstractExpression> to_expression(const std::shared_ptr<AbstractExpression>& expression) {
  return expression;
}

std::shared_ptr<LQPColumnExpression> to_expression(const LQPColumnReference& column_reference) {
  return std::make_shared<LQPColumnExpression>(column_reference);
}

std::shared_ptr<ValueExpression> to_expression(const AllTypeVariant& value) {
  return std::make_shared<ValueExpression>(value);
}

std::shared_ptr<ValueExpression> value(const AllTypeVariant& value) {
  return std::make_shared<ValueExpression>(value);
}

std::shared_ptr<ValueExpression> null() {
  return std::make_shared<ValueExpression>(NullValue{});
}

binary<ArithmeticOperator::Multiplication, ArithmeticExpression> multiplication;
binary<ArithmeticOperator::Addition, ArithmeticExpression> addition;

}  // namespace expression_factory

}  // namespace opossum