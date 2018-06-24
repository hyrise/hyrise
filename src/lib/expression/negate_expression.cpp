#include "negate_expression.hpp"

#include <sstream>

#include "utils/assert.hpp"

namespace opossum {

NegateExpression::NegateExpression(const std::shared_ptr<AbstractExpression>& argument):
AbstractExpression(ExpressionType::Negate, {argument}){
  Assert(argument->data_type() != DataType::String, "Can't negate strings");
}

std::shared_ptr<AbstractExpression> NegateExpression::argument() const {
  return arguments[0];
}

std::shared_ptr<AbstractExpression> NegateExpression::deep_copy() const {
  return std::make_shared<NegateExpression>(argument());
}

std::string NegateExpression::as_column_name() const {
  std::stringstream stream;
  stream << "-" << _enclose_argument_as_column_name(*argument());
  return stream.str();
}

DataType NegateExpression::data_type() const {
  return argument()->data_type();
}

}  // namespace opossum
