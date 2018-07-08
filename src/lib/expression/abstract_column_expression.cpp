#include "abstract_column_expression.hpp"

namespace opossum {

AbstractColumnExpression::AbstractColumnExpression() : AbstractExpression(ExpressionType::Column, {}) {}

bool AbstractColumnExpression::requires_computation() const {
  return false;
}

}  // namespace opossum
