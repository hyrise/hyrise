#include "value_expression.hpp"

#include <sstream>

namespace opossum {

ValueExpression::ValueExpression(const AllTypeVariant& value): AbstractExpression(ExpressionType::Value, {}), value(value) {

}

std::shared_ptr<AbstractExpression> ValueExpression::deep_copy() const  {
  return std::make_shared<ValueExpression>(value);
}

std::string ValueExpression::as_column_name() const {
  std::stringstream stream;

  Fail("Todo");
  return stream.str();
}

bool ValueExpression::_shallow_equals(const AbstractExpression& expression) const {
  return value == static_cast<const ValueExpression>(expression).value;
}

}