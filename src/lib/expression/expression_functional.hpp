#pragma once

#include <memory>

#include "abstract_expression.hpp"
#include "aggregate_expression.hpp"
#include "arithmetic_expression.hpp"
#include "between_expression.hpp"
#include "binary_predicate_expression.hpp"
#include "case_expression.hpp"
#include "cast_expression.hpp"
#include "exists_expression.hpp"
#include "extract_expression.hpp"
#include "function_expression.hpp"
#include "in_expression.hpp"
#include "is_null_expression.hpp"
#include "list_expression.hpp"
#include "logical_expression.hpp"
#include "lqp_column_expression.hpp"
#include "lqp_select_expression.hpp"
#include "parameter_expression.hpp"
#include "pqp_column_expression.hpp"
#include "pqp_select_expression.hpp"
#include "unary_minus_expression.hpp"
#include "value_expression.hpp"

/**
 * This file provides convenience methods to create (nested) Expression objects with little boilerplate.
 *
 * NOTE: functions suffixed with "_" (e.g. equals_()) to alert the unsuspecting reader to the fact this is something
 *       different than the equality check he might expect when reading "equals" *
 *
 * In Hyrise we say...
 *      case_(equals_(a, 123),
 *            b,
 *            case_(equals_(a, 1234),
 *                  a,
 *                  null_()))
 *
 * ...and it actually means:
 *     const auto value_123 = std::make_shared<ValueExpression>(123);
 *     const auto value_1234 = std::make_shared<ValueExpression>(1234);
 *     const auto a_eq_123 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, int_float_a_expression, value_123);
 *     const auto a_eq_1234 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, int_float_a_expression, value_1234);
 *     const auto null_value = std::make_shared<ValueExpression>(NullValue{});
 *     const auto case_a_eq_1234 = std::make_shared<CaseExpression>(a_eq_1234, int_float_a_expression, null_value);
 *     const auto case_a_eq_123 = std::make_shared<CaseExpression>(a_eq_123, int_float_b_expression, case_a_eq_1234);
 *
 *  ...and I think that's beautiful.
 */

namespace opossum {

class AbstractOperator;
class LQPColumnReference;

/**
 * expression_"functional", since it supplies a functional-programming like interface to build nested expressions
 */
namespace expression_functional {

/**
 * @defgroup Turn expression-like things (Values, LQPColumnReferences, Expressions themselves) into expressions
 *
 * Mostly used internally in this file
 *
 * @{
 */
std::shared_ptr<AbstractExpression> to_expression(const std::shared_ptr<AbstractExpression>& expression);
std::shared_ptr<LQPColumnExpression> to_expression(const LQPColumnReference& column_reference);
std::shared_ptr<ValueExpression> to_expression(const AllTypeVariant& value);
/** @} */

std::shared_ptr<ValueExpression> value_(const AllTypeVariant& value);
std::shared_ptr<ValueExpression> null_();

namespace detail {

/**
 * @defgroup Static objects that create Expressions that have an enum member (e.g. PredicateCondition::Equals)
 *
 * Having these eliminates the need to specify a function for each Expression-Enum-Member combination
 *
 * @{
 */

template <auto t, typename E>
struct unary final {
  template <typename A>
  std::shared_ptr<E> operator()(const A& a) const {
    return std::make_shared<E>(t, to_expression(a));
  }
};

template <auto t, typename E>
struct binary final {
  template <typename A, typename B>
  std::shared_ptr<E> operator()(const A& a, const B& b) const {
    return std::make_shared<E>(t, to_expression(a), to_expression(b));
  }
};

template <typename E>
struct ternary final {
  template <typename A, typename B, typename C>
  std::shared_ptr<E> operator()(const A& a, const B& b, const C& c) const {
    return std::make_shared<E>(to_expression(a), to_expression(b), to_expression(c));
  }
};

/** @} */

}  // namespace detail

inline detail::unary<PredicateCondition::IsNull, IsNullExpression> is_null_;
inline detail::unary<PredicateCondition::IsNotNull, IsNullExpression> is_not_null_;
inline detail::unary<AggregateFunction::Sum, AggregateExpression> sum_;
inline detail::unary<AggregateFunction::Max, AggregateExpression> max_;
inline detail::unary<AggregateFunction::Min, AggregateExpression> min_;
inline detail::unary<AggregateFunction::Avg, AggregateExpression> avg_;
inline detail::unary<AggregateFunction::Count, AggregateExpression> count_;
inline detail::unary<AggregateFunction::CountDistinct, AggregateExpression> count_distinct_;

inline detail::binary<ArithmeticOperator::Division, ArithmeticExpression> div_;
inline detail::binary<ArithmeticOperator::Multiplication, ArithmeticExpression> mul_;
inline detail::binary<ArithmeticOperator::Addition, ArithmeticExpression> add_;
inline detail::binary<ArithmeticOperator::Subtraction, ArithmeticExpression> sub_;
inline detail::binary<ArithmeticOperator::Modulo, ArithmeticExpression> mod_;
inline detail::binary<PredicateCondition::Like, BinaryPredicateExpression> like_;
inline detail::binary<PredicateCondition::NotLike, BinaryPredicateExpression> not_like_;
inline detail::binary<PredicateCondition::Equals, BinaryPredicateExpression> equals_;
inline detail::binary<PredicateCondition::NotEquals, BinaryPredicateExpression> not_equals_;
inline detail::binary<PredicateCondition::LessThan, BinaryPredicateExpression> less_than_;
inline detail::binary<PredicateCondition::LessThanEquals, BinaryPredicateExpression> less_than_equals_;
inline detail::binary<PredicateCondition::GreaterThanEquals, BinaryPredicateExpression> greater_than_equals_;
inline detail::binary<PredicateCondition::GreaterThan, BinaryPredicateExpression> greater_than_;
inline detail::binary<LogicalOperator::And, LogicalExpression> and_;
inline detail::binary<LogicalOperator::Or, LogicalExpression> or_;

inline detail::ternary<BetweenExpression> between_;
inline detail::ternary<CaseExpression> case_;

template <typename... Args>
std::shared_ptr<LQPSelectExpression> lqp_select_(const std::shared_ptr<AbstractLQPNode>& lqp,  // NOLINT
                                                 Args&&... parameter_id_expression_pairs) {
  if constexpr (sizeof...(Args) > 0) {
    // Correlated subselect
    return std::make_shared<LQPSelectExpression>(
        lqp, std::vector<ParameterID>{{parameter_id_expression_pairs.first...}},
        std::vector<std::shared_ptr<AbstractExpression>>{{to_expression(parameter_id_expression_pairs.second)...}});
  } else {
    // Not correlated
    return std::make_shared<LQPSelectExpression>(lqp, std::vector<ParameterID>{},
                                                 std::vector<std::shared_ptr<AbstractExpression>>{});
  }
}

template <typename... Args>
std::shared_ptr<PQPSelectExpression> pqp_select_(const std::shared_ptr<AbstractOperator>& pqp, const DataType data_type,
                                                 const bool nullable, Args&&... parameter_id_column_id_pairs) {
  if constexpr (sizeof...(Args) > 0) {
    // Correlated subselect
    return std::make_shared<PQPSelectExpression>(
        pqp, data_type, nullable,
        std::vector<std::pair<ParameterID, ColumnID>>{
            {std::make_pair(parameter_id_column_id_pairs.first, parameter_id_column_id_pairs.second)...}});
  } else {
    // Not correlated
    return std::make_shared<PQPSelectExpression>(pqp, data_type, nullable);
  }
}

template <typename... Args>
std::vector<std::shared_ptr<AbstractExpression>> expression_vector(Args&&... args) {
  return std::vector<std::shared_ptr<AbstractExpression>>({to_expression(args)...});
}

template <typename String, typename Start, typename Length>
std::shared_ptr<FunctionExpression> substr_(const String& string, const Start& start, const Length& length) {
  return std::make_shared<FunctionExpression>(
      FunctionType::Substring, expression_vector(to_expression(string), to_expression(start), to_expression(length)));
}

template <typename... Args>
std::shared_ptr<FunctionExpression> concat_(const Args... args) {
  return std::make_shared<FunctionExpression>(FunctionType::Concatenate, expression_vector(to_expression(args)...));
}

template <typename... Args>
std::shared_ptr<ListExpression> list_(Args&&... args) {
  return std::make_shared<ListExpression>(expression_vector(std::forward<Args>(args)...));
}

template <typename V, typename S>
std::shared_ptr<InExpression> in_(const V& v, const S& s) {
  return std::make_shared<InExpression>(to_expression(v), to_expression(s));
}

std::shared_ptr<ExistsExpression> exists_(const std::shared_ptr<AbstractExpression>& select_expression);

template <typename F>
std::shared_ptr<ExtractExpression> extract_(const DatetimeComponent datetime_component, const F& from) {
  return std::make_shared<ExtractExpression>(datetime_component, to_expression(from));
}

std::shared_ptr<ParameterExpression> uncorrelated_parameter_(const ParameterID parameter_id);
std::shared_ptr<LQPColumnExpression> lqp_column_(const LQPColumnReference& column_reference);
std::shared_ptr<PQPColumnExpression> pqp_column_(const ColumnID column_id, const DataType data_type,
                                                 const bool nullable, const std::string& column_name);

template <typename ReferencedExpression>
std::shared_ptr<ParameterExpression> correlated_parameter_(const ParameterID parameter_id,
                                                           const ReferencedExpression& referenced) {
  return std::make_shared<ParameterExpression>(parameter_id, *to_expression(referenced));
}

std::shared_ptr<AggregateExpression> count_star_();

template <typename Argument>
std::shared_ptr<UnaryMinusExpression> unary_minus_(const Argument& argument) {
  return std::make_shared<UnaryMinusExpression>(to_expression(argument));
}

template <typename Argument>
std::shared_ptr<CastExpression> cast_(const Argument& argument, const DataType data_type) {
  return std::make_shared<CastExpression>(to_expression(argument), data_type);
}

}  // namespace expression_functional

}  // namespace opossum
