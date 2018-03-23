#include "value_placeholder_expression.hpp"

namespace opossum {

ValuePlaceholderExpression::ValuePlaceholderExpression(const ValuePlaceholder& value_placeholder): AbstractExpression(ExpressionType::ValuePlaceholder), value_placeholder(value_placeholder) {

}

bool ValuePlaceholderExpression::deep_equals(const AbstractExpression& expression) const {
  if (type != expression.type) return false;
  const auto& value_expression = static_cast<const ValuePlaceholderExpression&>(expression);
  return value_expression.value_placeholder == value_placeholder;
}

std::shared_ptr<AbstractExpression> ValuePlaceholderExpression::deep_copy() const  {
  return std::make_shared<ValuePlaceholderExpression>(value_placeholder);
}

std::shared_ptr<AbstractExpression> ValuePlaceholderExpression::deep_resolve_column_expressions()  {
  return shared_from_this();
}

}