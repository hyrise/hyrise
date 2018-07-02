#include "abstract_column_expression.hpp"

namespace opossum {

AbstractColumnExpression::AbstractColumnExpression() : AbstractExpression(ExpressionType::Column, {}) {}

}  // namespace opossum
