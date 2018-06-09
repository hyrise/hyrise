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

std::shared_ptr<ValuePlaceholderExpression> value_placeholder(const ValuePlaceholder& value_placeholder) {
  return std::make_shared<ValuePlaceholderExpression>(value_placeholder);
}

std::shared_ptr<ValuePlaceholderExpression> value_placeholder(const uint16_t index) {
  return std::make_shared<ValuePlaceholderExpression>(ValuePlaceholder{index});
}

std::shared_ptr<ValueExpression> null() {
  return std::make_shared<ValueExpression>(NullValue{});
}

std::shared_ptr<ParameterExpression> parameter(const ParameterID parameter_id) {
  return std::make_shared<ParameterExpression>(parameter_id);
}

std::shared_ptr<AggregateExpression> count_star() {
  return std::make_shared<AggregateExpression>(AggregateFunction::Count);
}

unary<PredicateCondition::IsNull, IsNullExpression> is_null;
unary<PredicateCondition::IsNotNull, IsNullExpression> is_not_null;
unary<AggregateFunction::Sum, AggregateExpression> sum;
unary<AggregateFunction::Max, AggregateExpression> max;
unary<AggregateFunction::Min, AggregateExpression> min;
unary<AggregateFunction::Avg, AggregateExpression> avg;
unary<AggregateFunction::Count, AggregateExpression> count;
unary<AggregateFunction::CountDistinct, AggregateExpression> count_distinct;
binary<ArithmeticOperator::Multiplication, ArithmeticExpression> mul;
binary<ArithmeticOperator::Division, ArithmeticExpression> division;
binary<ArithmeticOperator::Addition, ArithmeticExpression> add;
binary<ArithmeticOperator::Subtraction, ArithmeticExpression> sub;
binary<PredicateCondition::Equals, BinaryPredicateExpression> equals;
binary<PredicateCondition::NotEquals, BinaryPredicateExpression> not_equals;
binary<PredicateCondition::LessThan, BinaryPredicateExpression> less_than;
binary<PredicateCondition::LessThanEquals, BinaryPredicateExpression> less_than_equals;
binary<PredicateCondition::GreaterThanEquals, BinaryPredicateExpression> greater_than_equals;
binary<PredicateCondition::GreaterThan, BinaryPredicateExpression> greater_than;
binary<LogicalOperator::And, LogicalExpression> and_;
binary<LogicalOperator::Or, LogicalExpression> or_;
ternary<BetweenExpression> between;
ternary<CaseExpression> case_;

}  // namespace expression_factory

}  // namespace opossum