#include "abstract_select_expression.hpp"

namespace opossum {

AbstractSelectExpression::AbstractSelectExpression():
 AbstractExpression(ExpressionType::Select, {}) {}

bool AbstractSelectExpression::requires_calculation() const {
  return true;
}

}  // namespace opossum
