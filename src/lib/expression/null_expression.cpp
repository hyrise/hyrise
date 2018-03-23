#include "null_expression.hpp"

namespace opossum {

NullExpression::NullExpression(): AbstractExpression(ExpressionType::Null) {

}

bool NullExpression::deep_equals(const AbstractExpression& expression) const {
  return type == expression.type;
}

std::shared_ptr<AbstractExpression> NullExpression::deep_copy() const {
  return std::make_shared<NullExpression>();
}

std::shared_ptr<AbstractExpression> NullExpression::deep_resolve_column_expressions() {
  return shared_from_this();
}

}  // namespace opossum
