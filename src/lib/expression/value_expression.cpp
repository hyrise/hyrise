#include "value_expression.hpp"

#include <sstream>

#include "resolve_data_type.hpp"

namespace opossum {

ValueExpression::ValueExpression(const AllTypeVariant& value)
    : AbstractExpression(ExpressionType::Value, {}), value(value) {}

bool ValueExpression::requires_computation() const { return false; }

std::shared_ptr<AbstractExpression> ValueExpression::deep_copy() const {
  return std::make_shared<ValueExpression>(value);
}

std::string ValueExpression::as_column_name() const {
  std::stringstream stream;

  if (std::holds_alternative<std::string>(value)) {
    stream << "'" << value << "'";
  } else {
    stream << value;
  }

  if (std::holds_alternative<int64_t>(value)) {
    stream << "l";
  } else if (std::holds_alternative<float>(value)) {
    stream << "f";
  }

  return stream.str();
}

DataType ValueExpression::data_type() const { return data_type_from_all_type_variant(value); }

bool ValueExpression::is_nullable() const { return std::holds_alternative<NullValue>(value); }

bool ValueExpression::_shallow_equals(const AbstractExpression& expression) const {
  const auto& value_expression = static_cast<const ValueExpression&>(expression);

  /**
   * Even though null != null, two null expressions are *the same expressions* (e.g. when resolving ColumnIDs)
   */
  if (data_type() == DataType::Null && value_expression.data_type() == DataType::Null) return true;

  return value == value_expression.value;
}

size_t ValueExpression::_on_hash() const { return std::hash<AllTypeVariant>{}(value); }

}  // namespace opossum
