#include "not_expression.hpp"

#include "utils/assert.hpp"

namespace opossum {

NotExpression::NotExpression(const std::shared_ptr<AbstractExpression>& operand):
  AbstractExpression(ExpressionType::Not, {operand}){

}

const std::shared_ptr<AbstractExpression>& NotExpression::operand() const {
  return arguments[0];
}

std::shared_ptr<AbstractExpression> NotExpression::deep_copy() const {
  return std::make_shared<NotExpression>(operand()->deep_copy()):
}

std::string NotExpression::description() const {
  Fail("Notyetimplemented");
  return "";
}

}  // namespace opossum
