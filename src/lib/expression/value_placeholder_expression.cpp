#include "value_placeholder_expression.hpp"

#include "boost/functional/hash.hpp"

#include <sstream>

namespace opossum {

ValuePlaceholderExpression::ValuePlaceholderExpression(const ValuePlaceholder& value_placeholder): AbstractExpression(ExpressionType::ValuePlaceholder, {}), value_placeholder(value_placeholder) {

}

std::shared_ptr<AbstractExpression> ValuePlaceholderExpression::deep_copy() const  {
  return std::make_shared<ValuePlaceholderExpression>(value_placeholder);
}

std::string ValuePlaceholderExpression::as_column_name() const {
  std::stringstream stream;

  Fail("Todo");
  return stream.str();
}

bool ValuePlaceholderExpression::_shallow_equals(const AbstractExpression& expression) const {
  return value_placeholder == static_cast<const ValuePlaceholderExpression&>(expression).value_placeholder;
}

size_t ValuePlaceholderExpression::_on_hash() const {
  return std::hash<ValuePlaceholder>{}(value_placeholder);
}

}