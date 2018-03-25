#include "value_placeholder_expression.hpp"

#include <sstream>

namespace opossum {

ValuePlaceholderExpression::ValuePlaceholderExpression(const ValuePlaceholder& value_placeholder): AbstractExpression(ExpressionType::ValuePlaceholder, {}), value_placeholder(value_placeholder) {

}

std::shared_ptr<AbstractExpression> ValuePlaceholderExpression::deep_copy() const  {
  return std::make_shared<ValuePlaceholderExpression>(value_placeholder);
}

std::string ValuePlaceholderExpression::description() const {
  std::stringstream stream;

  Fail("Todo");
  return stream.str();
}

bool ValuePlaceholderExpression::_shallow_equals(const AbstractExpression& expression) const {
  return value_placeholder == static_cast<const ValuePlaceholderExpression&>(expression).value_placeholder;
}

}