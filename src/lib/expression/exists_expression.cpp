#include "exists_expression.hpp"

#include "select_expression.hpp"

namespace opossum {

ExistsExpression::ExistsExpression(const std::shared_ptr<SelectExpression>& select):
  AbstractExpression(ExpressionType::Exists), select(select) {

}

bool ExistsExpression::deep_equals(const AbstractExpression& expression) const {
  if (type != expression.type) return false;

  const auto& exists_expression = static_cast<const ExistsExpression&>(expression);
  return select->deep_equals(*exists_expression.select);
}

std::shared_ptr<AbstractExpression> ExistsExpression::deep_copy() const {
  return std::make_shared<ExistsExpression>(select->deep_copy());
}

std::shared_ptr<AbstractExpression> ExistsExpression::deep_resolve_column_expressions() {
  select = select.deep_resolve_column_expressions();
  return shared_from_this();
}

}  // namespace opossum
