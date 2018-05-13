#include "expression_factory.hpp"

#include "expression/lqp_select_expression.hpp"

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

std::shared_ptr<ValuePlaceholderExpression> value_placeholder(const ValuePlaceholder& value_placeholder) {
  return std::make_shared<ValuePlaceholderExpression>(value_placeholder);
}

std::shared_ptr<ValuePlaceholderExpression> value_placeholder(const uint16_t index) {
  return std::make_shared<ValuePlaceholderExpression>(ValuePlaceholder{index});
}

std::shared_ptr<ValueExpression> null() {
  return std::make_shared<ValueExpression>(NullValue{});
}

std::shared_ptr<AbstractExpression> select(const std::shared_ptr<AbstractLQPNode>& lqp, const std::vector<std::shared_ptr<AbstractExpression>>& referenced_external_expressions) {
  return std::make_shared<LQPSelectExpression>(lqp, referenced_external_expressions);
}

unary<AggregateFunction::Sum, AggregateExpression> sum;
unary<AggregateFunction::Min, AggregateExpression> min;
binary<ArithmeticOperator::Multiplication, ArithmeticExpression> multiplication;
binary<ArithmeticOperator::Division, ArithmeticExpression> division;
binary<ArithmeticOperator::Addition, ArithmeticExpression> addition;
binary<PredicateCondition::Equals, BinaryPredicateExpression> equals;
binary<PredicateCondition::LessThan, BinaryPredicateExpression> less_than;
binary<PredicateCondition::GreaterThanEquals, BinaryPredicateExpression> greater_than_equals;
binary<PredicateCondition::GreaterThan, BinaryPredicateExpression> greater_than;
ternary<BetweenExpression> between;
ternary<CaseExpression> case_;

}  // namespace expression_factory

}  // namespace opossum