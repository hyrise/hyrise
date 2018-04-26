#include "expression_evaluator.hpp"

#include "abstract_expression.hpp"
#include "arithmetic_expression.hpp"
#include "pqp_column_expression.hpp"
#include "storage/materialize.hpp"
#include "resolve_type.hpp"

namespace opossum {

template<typename T>
ExpressionEvaluator::ExpressionResult<T> ExpressionEvaluator::evaluate_expression(const AbstractExpression& expression) const {
  switch (expression.type) {
    case ExpressionType::Arithmetic:
      return evaluate_arithmetic_expression<T>(static_cast<const ArithmeticExpression&>(expression));

    case ExpressionType::Column: {
      const auto* pqp_column_expression = dynamic_cast<const PQPColumnExpression*>(&expression);
      Assert(pqp_column_expression, "Can only evaluate PQPColumnExpressions");

      const auto column = _chunk->get_column(pqp_column_expression->column_id);

      std::vector<T> values;
      materialize_values(*column, values);

      return values;
    }

    default:
      Fail("ExpressionType evaluation not yet implemented");
  }
}

template<typename T>
ExpressionEvaluator::ExpressionResult<T> ExpressionEvaluator::evaluate_arithmetic_expression(const ArithmeticExpression& expression) const {
  switch (expression.arithmetic_operator) {
    case ArithmeticOperator::Addition:
      return evaluate_binary_expression<T>(*expression.left_operand(), *expression.right_operand(), std::plus<T>{});


    default:
      Fail("ArithmeticOperator evaluation not yet implemented");
  }
}

template<typename T, typename OperatorFunctor>
ExpressionEvaluator::ExpressionResult<T> ExpressionEvaluator::evaluate_binary_expression(
const AbstractExpression& left_operand,
const AbstractExpression& right_operand,
const OperatorFunctor &functor) const {
  const auto left_data_type = get_expression_data_type(left_operand);
  const auto right_data_type = get_expression_data_type(right_operand);

  ExpressionResult<T> result;

  resolve_data_type(left_data_type, [&](const auto left_data_type_t) {
    using LeftDataType = typename decltype(left_data_type_t)::type;

    const auto left_operands = evaluate_expression<LeftDataType>(left_operand);

    resolve_data_type(right_data_type, [&](const auto right_data_type_t) {
      using RightDataType = typename decltype(right_data_type_t)::type;

      const auto right_operands = evaluate_expression<RightDataType>(right_operand);

      result = evaluate_binary_operator<T, LeftDataType, RightDataType>(left_operands, right_operands, functor);
    });
  });

  return result;
}

template<typename ResultDataType,
         typename LeftOperandDataType,
         typename RightOperandDataType,
         typename Functor>
ExpressionEvaluator::ExpressionResult<ResultDataType> ExpressionEvaluator::evaluate_binary_operator(const ExpressionResult<LeftOperandDataType>& left_operands,
                                                                                                    const ExpressionResult<RightOperandDataType>& right_operands,
                                                                                                    const Functor &functor) {
  if (left_operands.type() == typeid(NullValue)) return NullValue{};
  if (right_operands.type() == typeid(NullValue)) return NullValue{};

  const auto left_values = boost::get<NonNullableValues<LeftOperandDataType>>(left_operands);
  const auto right_values = boost::get<NonNullableValues<RightOperandDataType>>(right_operands);
  const auto result_size = left_values.size();

  NonNullableValues<ResultDataType> result_values(result_size);

  // clang-format off
  constexpr auto result_is_string = std::is_same_v<ResultDataType, std::string>;
  constexpr auto left_is_string = std::is_same_v<LeftOperandDataType, std::string>;
  constexpr auto right_is_string = std::is_same_v<RightOperandDataType, std::string>;

  if constexpr (result_is_string || left_is_string || right_is_string) {
    Fail("Strings not supported yet");
  } else {
    for (auto value_idx = size_t{0}; value_idx < result_size; ++value_idx) {
      result_values[value_idx] = functor(static_cast<ResultDataType>(left_values[value_idx]),
                                         static_cast<ResultDataType>(right_values[value_idx]));
    }
  }
  // clang-format on

  return result_values;
}

}  // namespace opossum
