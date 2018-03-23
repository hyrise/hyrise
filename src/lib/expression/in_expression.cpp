#include "in_expression.hpp"

namespace opossum {

InExpression::InExpression(const std::shared_ptr<AbstractExpression>& value, const std::shared_ptr<AbstractExpression>& set):
  AbstractExpression(ExpressionType::In), value(value), set(set) {}

bool InExpression::deep_equals(const AbstractExpression& expression) const {
  if (type != expression.type) return false;

  const auto& in_expression = static_cast<const InExpression&>(expression);

  return value->deep_equals(*in_expression.value) && set->deep_equals(*in_expression.set);
}

std::shared_ptr<AbstractExpression> InExpression::deep_copy() const {
  return std::make_shared<AbstractExpression>(value->deep_copy(), set->deep_copy());
}

std::shared_ptr<AbstractExpression> InExpression::deep_resolve_column_expressions() override {
  value = value->deep_resolve_column_expressions();
  set = set->deep_resolve_column_expressions();
  return shared_from_this();
}

}  // namespace opossum
