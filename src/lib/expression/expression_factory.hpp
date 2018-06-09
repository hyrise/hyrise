#pragma once

#include <memory>

#include "abstract_expression.hpp"
#include "array_expression.hpp"
#include "aggregate_expression.hpp"
#include "arithmetic_expression.hpp"
#include "extract_expression.hpp"
#include "between_expression.hpp"
#include "binary_predicate_expression.hpp"
#include "is_null_expression.hpp"
#include "function_expression.hpp"
#include "lqp_select_expression.hpp"
#include "case_expression.hpp"
#include "external_expression.hpp"
#include "in_expression.hpp"
#include "logical_expression.hpp"
#include "lqp_column_expression.hpp"
#include "parameter_expression.hpp"
#include "value_expression.hpp"
#include "value_placeholder_expression.hpp"

/**
 * This file provides convenience methods to create (nested) Expression objects with little boilerplate
 *
 * So this
 *     const auto value_123 = std::make_shared<ValueExpression>(123);
 *     const auto value_1234 = std::make_shared<ValueExpression>(1234);
 *     const auto a_eq_123 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, int_float_a_expression, value_123);
 *     const auto a_eq_1234 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, int_float_a_expression, value_1234);
 *     const auto null_value = std::make_shared<ValueExpression>(NullValue{});
 *     const auto case_a_eq_1234 = std::make_shared<CaseExpression>(a_eq_1234, int_float_a_expression, null_value);
 *     const auto case_a_eq_123 = std::make_shared<CaseExpression>(a_eq_123, int_float_b_expression, case_a_eq_1234);
 *
 *  becomes
 *      case_(equals(a, 123),
 *            b,
 *            case_(equals(a, 1234),
 *                  a,
 *                  null()))
 */


namespace opossum {

class LQPColumnReference;

namespace expression_factory  {

// to_expression() overload that just forwards
std::shared_ptr<AbstractExpression> to_expression(const std::shared_ptr<AbstractExpression>& expression);

std::shared_ptr<LQPColumnExpression> to_expression(const LQPColumnReference& column_reference);
std::shared_ptr<ValueExpression> to_expression(const AllTypeVariant& value);

std::shared_ptr<ValueExpression> value(const AllTypeVariant& value);
std::shared_ptr<ValuePlaceholderExpression> value_placeholder(const ValuePlaceholder& value_placeholder);
std::shared_ptr<ValuePlaceholderExpression> value_placeholder(const uint16_t index);
std::shared_ptr<ValueExpression> null();

template<auto t, typename E>
struct unary final {
  template<typename A>
  std::shared_ptr<E> operator()(const A &a) {
    return std::make_shared<E>(t, to_expression(a));
  };
};

template<auto t, typename E>
struct binary final {
  template<typename A, typename B>
  std::shared_ptr<E> operator()(const A &a, const B &b) {
    return std::make_shared<E>(t, to_expression(a), to_expression(b));
  };
};

template<typename E>
struct ternary final {
  template<typename A, typename B, typename C>
  std::shared_ptr<E> operator()(const A& a, const B& b, const C& c) {
    return std::make_shared<E>(to_expression(a), to_expression(b), to_expression(c));
  };
};

extern unary<PredicateCondition::IsNull, IsNullExpression> is_null;
extern unary<PredicateCondition::IsNotNull, IsNullExpression> is_not_null;
extern unary<AggregateFunction::Sum, AggregateExpression> sum;
extern unary<AggregateFunction::Max, AggregateExpression> max;
extern unary<AggregateFunction::Min, AggregateExpression> min;
extern unary<AggregateFunction::Avg, AggregateExpression> avg;
extern unary<AggregateFunction::Count, AggregateExpression> count;
extern unary<AggregateFunction::CountDistinct, AggregateExpression> count_distinct;
extern binary<ArithmeticOperator::Division, ArithmeticExpression> division;
extern binary<ArithmeticOperator::Multiplication, ArithmeticExpression> mul;
extern binary<ArithmeticOperator::Addition, ArithmeticExpression> add;
extern binary<ArithmeticOperator::Subtraction, ArithmeticExpression> sub;
extern binary<PredicateCondition::Equals, BinaryPredicateExpression> equals;
extern binary<PredicateCondition::NotEquals, BinaryPredicateExpression> not_equals;
extern binary<PredicateCondition::LessThan, BinaryPredicateExpression> less_than;
extern binary<PredicateCondition::LessThanEquals, BinaryPredicateExpression> less_than_equals;
extern binary<PredicateCondition::GreaterThanEquals, BinaryPredicateExpression> greater_than_equals;
extern binary<PredicateCondition::GreaterThan, BinaryPredicateExpression> greater_than;
extern binary<LogicalOperator::And, LogicalExpression> and_;
extern binary<LogicalOperator::Or, LogicalExpression> or_;
extern ternary<BetweenExpression> between;
extern ternary<CaseExpression> case_;

template<typename ... Args>
std::shared_ptr<AbstractExpression> select(const std::shared_ptr<AbstractLQPNode>& lqp,
                                           Args &&... value_placeholder_expression_pairs) {
  return std::make_shared<LQPSelectExpression>(
    lqp,
    std::vector<std::pair<ValuePlaceholder, std::shared_ptr<AbstractExpression>>>{{std::make_pair(value_placeholder_expression_pairs.first, to_expression(value_placeholder_expression_pairs.second))...}});
}

template<typename E>
std::shared_ptr<ExternalExpression> external(const E& e, const uint16_t index) {
  const auto expression = to_expression(e);
  return std::make_shared<ExternalExpression>(ValuePlaceholder{index}, expression->data_type(), expression->is_nullable(), expression->as_column_name());
}

template<typename ... Args>
std::vector<std::shared_ptr<AbstractExpression>> expression_vector(Args &&... args) {
  return std::vector<std::shared_ptr<AbstractExpression>>({
    to_expression(args)...
  });
}

template<typename String, typename Start, typename Length>
std::shared_ptr<AbstractExpression> substr(const String& string, const Start& start, const Length& length) {
  return std::make_shared<FunctionExpression>(FunctionType::Substring, expression_vector(to_expression(string), to_expression(start), to_expression(length)));
}

template<typename ... Args>
std::shared_ptr<ArrayExpression> array(Args &&... args) {
  return std::make_shared<ArrayExpression>(expression_vector(std::forward<Args>(args)...));
}

template<typename V, typename S>
std::shared_ptr<InExpression> in(const V& v, const S& s) {
  return std::make_shared<InExpression>(to_expression(v), to_expression(s));
}

template<typename F>
std::shared_ptr<ExtractExpression> extract(const DatetimeComponent datetime_component, const F& from) {
  return std::make_shared<ExtractExpression>(datetime_component, to_expression(from));
}

template<typename ReferencedExpression>
std::shared_ptr<ParameterExpression> parameter(const ParameterID parameter_id, const ReferencedExpression& referenced) {
  return std::make_shared<ParameterExpression>(parameter_id, *to_expression(referenced));
}

std::shared_ptr<AggregateExpression> count_star();

}  // namespace expression_factory

}  // namespace opossum
