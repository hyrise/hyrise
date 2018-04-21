#pragma once

#include <memory>
#include <vector>

#include "boost/variant.hpp"

#include "types.hpp"
#include "null_value.hpp"

namespace opossum {

class AbstractExpression;
class ArithmeticExpression;
class PredicateExpression;
class Chunk;

class ExpressionEvaluator final {
 public:
  template<typename T> using NullableValues = std::pair<std::vector<bool>, std::vector<T>>;
  template<typename T> using NonNullableValues = std::vector<T>;

  template<typename T> using ExpressionResult = boost::variant<
    NullableValues<T>,
    NonNullableValues<T>,
    T,
    NullValue
  >;

  explicit ExpressionEvaluator(const std::shared_ptr<Chunk>& chunk);

  template<typename T>
  ExpressionResult<T> evaluate_expression(const AbstractExpression& expression) const;

  template<typename T>
  ExpressionResult<T> evaluate_arithmetic_expression(const ArithmeticExpression& expression) const;

  template<typename T, typename OperatorFunctor>
  static ExpressionResult<T> evaluate_binary_operator(const ExpressionResult<T>& left_operand,
                                                      const ExpressionResult<T>& right_operand,
                                                      const OperatorFunctor& functor);

 private:
  std::shared_ptr<Chunk> _chunk;
};

}  // namespace opossum

#include "expression_evaluator.ipp"