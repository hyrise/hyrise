#include "abstract_select_expression.hpp"

namespace opossum {

AbstractSelectExpression::AbstractSelectExpression():
 AbstractExpression(ExpressionType::Select, {}) {}

AbstractSelectExpression::AbstractSelectExpression(const std::vector<std::shared_ptr<AbstractExpression>>& referenced_external_expressions):
AbstractExpression(ExpressionType::Select, referenced_external_expressions) {}

bool AbstractSelectExpression::requires_calculation() const {
  return true;
}

}  // namespace opossum
