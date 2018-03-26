#include "lqp_column_expression.hpp"

#include "utils/assert.hpp"

namespace opossum {

LQPColumnExpression::LQPColumnExpression(const QualifiedColumnName& qualified_column_name,
                                         const std::optional<LQPColumnReference>& column_reference):
  qualified_column_name(qualified_column_name),
  column_reference(column_reference) {}

std::shared_ptr<AbstractExpression> LQPColumnExpression::deep_copy() const {
  return std::make_shared<LQPColumnExpression>(qualified_column_name, column_reference);
}

std::string LQPColumnExpression::description() const {
  Fail("TODO");
}

bool LQPColumnExpression::_shallow_equals(const AbstractExpression& expression) const {
  const auto& lqp_column_expression = static_cast<const LQPColumnExpression&>(expression);

  // TODO(moritz) explain what you're doing here.

  if (column_reference || lqp_column_expression.column_reference) {
    return column_reference == lqp_column_expression.column_reference;
  }

  if (qualified_column_name.table_name || lqp_column_expression.qualified_column_name.table_name) {
    return qualified_column_name == lqp_column_expression.qualified_column_name;
  }

  return qualified_column_name.table_name == lqp_column_expression.qualified_column_name.table_name;
}

}  // namespace opossum
