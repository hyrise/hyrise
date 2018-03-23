#include "value_expression.hpp"

namespace opossum {

ValueExpression::ValueExpression(const AllTypeVariant& value): AbstractExpression(ExpressionType::Value), value(value) {

}

bool ValueExpression::deep_equals(const AbstractExpression& expression) const {
  if (type != expression.type) return false;
  const auto& value_expression = static_cast<const ValueExpression&>(expression);
  return value_expression.value == value;
}

std::shared_ptr<AbstractExpression> ValueExpression::deep_copy() const  {
  return std::make_shared<ValueExpression>(value);
}

std::shared_ptr<AbstractExpression> ValueExpression::deep_resolve_column_expressions()  {
  return shared_from_this();
}

}