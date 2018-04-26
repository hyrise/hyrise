#include "arithmetic_expression.hpp"

#include <sstream>

#include "boost/functional/hash.hpp"

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

ExpressionDataTypeVariant ArithmeticExpression::data_type() const {
  const auto left = left_operand()->data_type();
  const auto right = right_operand()->data_type();

  if (is_invalid_arguments(left) || is_invalid_arguments(right)) return ExpressionDataTypeInvalidArguments{};
  if (is_vacant(left) || is_vacant(right)) return ExpressionDataTypeVacant{};

  const auto left_data_type = boost::get<DataType>(left_operand()->data_type());
  const auto right_data_type = boost::get<DataType>(right_operand()->data_type());

  Assert((left_data_type == DataType::String) == (right_data_type == DataType::String), "Strings only compatible with strings");

  if (left_data_type == DataType::Double || right_data_type == DataType::Double) return DataType::Double;
  if (left_data_type == DataType::Long) {
    return is_floating_point_data_type(right_data_type) ? DataType::Double : DataType::Long;
  }
  if (right_data_type == DataType::Long) {
    return is_floating_point_data_type(left_data_type) ? DataType::Double : DataType::Long;
  }
  if (left_data_type == DataType::Float || right_data_type == DataType::Float) return DataType::Float;

  return DataType::Int;
}

std::string ArithmeticExpression::as_column_name() const {
  std::stringstream stream;

  Fail("Todo");

  return stream.str();
}

bool ArithmeticExpression::_shallow_equals(const AbstractExpression& expression) const {
  return arithmetic_operator == static_cast<const ArithmeticExpression&>(expression).arithmetic_operator;
}

size_t ArithmeticExpression::_on_hash() const {
  return boost::hash_value(static_cast<size_t>(arithmetic_operator));
}

}  // namespace opossum
