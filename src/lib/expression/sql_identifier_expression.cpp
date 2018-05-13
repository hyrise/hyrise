#include "sql_identifier_expression.hpp"

namespace opossum {

SQLIdentifierExpression::SQLIdentifierExpression(const SQLIdentifier& sql_identifier):
sql_identifier(sql_identifier) {}

std::shared_ptr<AbstractExpression> SQLIdentifierExpression::deep_copy() const {
  return std::make_shared<SQLIdentifierExpression>(sql_identifier);
}

std::string SQLIdentifierExpression::as_column_name() const {
  Fail("Lol");
}

DataType SQLIdentifierExpression::data_type() const {
  Fail("Can't deduce the DataType of an SQL identifier");
}

bool SQLIdentifierExpression::_shallow_equals(const AbstractExpression& expression) const {
  const auto* sql_identifier_expression = dynamic_cast<const SQLIdentifierExpression*>(&expression);
  if (!sql_identifier_expression) return false;
  return sql_identifier == sql_identifier_expression->sql_identifier;
}

size_t SQLIdentifierExpression::_on_hash() const {
  return std::hash<SQLIdentifier>{}(sql_identifier);
}

}  // namespace opossum
