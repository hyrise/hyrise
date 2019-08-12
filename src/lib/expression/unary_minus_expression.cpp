#include "unary_minus_expression.hpp"

#include <sstream>

#include "utils/assert.hpp"

namespace opossum {

UnaryMinusExpression::UnaryMinusExpression(const std::shared_ptr<AbstractExpression>& argument)
    : AbstractExpression(ExpressionType::UnaryMinus, {argument}) {
  Assert(argument->data_type() != DataType::String, "Can't negate strings");
}

std::shared_ptr<AbstractExpression> UnaryMinusExpression::argument() const { return arguments[0]; }

std::shared_ptr<AbstractExpression> UnaryMinusExpression::deep_copy() const {
  return std::make_shared<UnaryMinusExpression>(argument());
}

std::string UnaryMinusExpression::as_column_name() const {
  std::stringstream stream;
  stream << "-" << _enclose_argument_as_column_name(*argument());
  return stream.str();
}

DataType UnaryMinusExpression::data_type() const { return argument()->data_type(); }

bool UnaryMinusExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const UnaryMinusExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  return true;
}

}  // namespace opossum
