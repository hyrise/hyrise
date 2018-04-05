#include "abstract_expression.hpp"

#include <queue>

namespace opossum {

AbstractExpression::AbstractExpression(const ExpressionType type, const std::vector<std::shared_ptr<AbstractExpression>>& arguments):
  type(type), arguments(arguments) {

}

bool AbstractExpression::requires_calculation() const {
  return !arguments.empty();
}

bool AbstractExpression::deep_equals(const AbstractExpression& expression) const {
  if (type != expression.type) return false;
  if (!deep_equals_expressions(arguments, expression.arguments)) return false;
  return _shallow_equals(expression);
}

bool AbstractExpression::_shallow_equals(const AbstractExpression& expression) const {
  return true;
}

}  // namespace opoosum