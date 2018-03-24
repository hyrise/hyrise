#include "value_expression.hpp"

namespace opossum {

ValueExpression::ValueExpression(const AllTypeVariant& value): AbstractExpression(ExpressionType::Value, {}), value(value) {

}

std::shared_ptr<AbstractExpression> ValueExpression::deep_copy() const  {
  return std::make_shared<ValueExpression>(value);
}

bool ValueExpression::_shallow_equals(const AbstractExpression& expression) const {
  return value == static_cast<const ValueExpression>(expression).value;
}

}