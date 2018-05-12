#pragma once

#include <memory>

#include "abstract_expression.hpp"
#include "arithmetic_expression.hpp"
#include "binary_predicate_expression.hpp"
#include "case_expression.hpp"
#include "lqp_column_expression.hpp"
#include "value_expression.hpp"

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
 *
 * Intended for tests, etc.
 */


namespace opossum {

class LQPColumnReference;

namespace expression_factory  {

// to_expression() overload that just forwards
std::shared_ptr<AbstractExpression> to_expression(const std::shared_ptr<AbstractExpression>& expression);

std::shared_ptr<LQPColumnExpression> to_expression(const LQPColumnReference& column_reference);
std::shared_ptr<ValueExpression> to_expression(const AllTypeVariant& value);

std::shared_ptr<ValueExpression> value(const AllTypeVariant& value);
std::shared_ptr<ValueExpression> null();

template<auto t, typename E>
struct binary final {
  template<typename L, typename R>
  std::shared_ptr<E> operator()(const L &lhs, const R &rhs) {
    return std::make_shared<E>(t, to_expression(lhs), to_expression(rhs));
  };
};

extern binary<ArithmeticOperator::Multiplication, ArithmeticExpression> multiplication;
extern binary<ArithmeticOperator::Addition, ArithmeticExpression> addition;
extern binary<PredicateCondition::Equals, BinaryPredicateExpression> equals;
extern binary<PredicateCondition::LessThan, BinaryPredicateExpression> less_than;
extern binary<PredicateCondition::GreaterThanEquals, BinaryPredicateExpression> greater_than_equals;
extern binary<PredicateCondition::GreaterThan, BinaryPredicateExpression> greater_than;

template<typename ExpressionLikeWhen, typename ExpressionLikeThen, typename ExpressionLikeElse>
std::shared_ptr<CaseExpression> case_(const ExpressionLikeWhen& when,
                                      const ExpressionLikeThen& then,
                                      const ExpressionLikeElse& else_) {
  return std::make_shared<CaseExpression>(to_expression(when), to_expression(then), to_expression(else_));
};

template<typename ... Args>
std::vector<std::shared_ptr<AbstractExpression>> expression_vector(Args &&... args) {
  return std::vector<std::shared_ptr<AbstractExpression>>({
    to_expression(args)...
  });
}

}  // namespace expression_factory

}  // namespace opossum
