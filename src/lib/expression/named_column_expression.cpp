#include "named_column_expression.hpp"

namespace opossum {

NamedColumnExpression::NamedColumnExpression(const ColumnIdentifier& qualified_column_name):
  qualified_column_name(qualified_column_name) {}

std::shared_ptr<AbstractExpression> NamedColumnExpression::deep_copy() const {
  return std::make_shared<NamedColumnExpression>(qualified_column_name);
}

std::string NamedColumnExpression::as_column_name() const {
  Fail("Lol");
}

bool NamedColumnExpression::_shallow_equals(const AbstractExpression& expression) const {
  return qualified_column_name == static_cast<const NamedColumnExpression&>(expression).qualified_column_name;
}

size_t NamedColumnExpression::_on_hash() const {
  return std::hash<ColumnIdentifier>{}(qualified_column_name);
}

}  // namespace opossum
