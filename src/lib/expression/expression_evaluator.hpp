#pragma once

#include <memory>
#include <vector>

#include "boost/variant.hpp"

#include "types.hpp"
#include "all_type_variant.hpp"
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

  DataType get_expression_data_type(const AbstractExpression& expression) const;

  template<typename T>
  ExpressionResult<T> evaluate_expression(const AbstractExpression& expression) const;

  template<typename T>
  ExpressionResult<T> evaluate_arithmetic_expression(const ArithmeticExpression& expression) const;

  template<typename T, typename OperatorFunctor>
  ExpressionResult<T> evaluate_binary_expression(const AbstractExpression& left_operand,
                                                        const AbstractExpression& right_operand,
                                                        const OperatorFunctor &functor) const;
  template<typename ResultDataType,
           typename LeftOperandDataType,
           typename RightOperandDataType,
           typename Functor>
  static ExpressionResult<ResultDataType> evaluate_binary_operator(const ExpressionResult<LeftOperandDataType>& left_operands,
                                                                   const ExpressionResult<RightOperandDataType>& right_operands,
                                                                   const Functor &functor);

 private:
  std::shared_ptr<Chunk> _chunk;
};

}  // namespace opossum

#include "expression_evaluator.inl"