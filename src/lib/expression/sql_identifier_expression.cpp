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

ExpressionDataTypeVariant SQLIdentifierExpression::data_type() const {
  return ExpressionDataTypeVacant{};
}

bool SQLIdentifierExpression::_shallow_equals(const AbstractExpression& expression) const {
  return sql_identifier == static_cast<const SQLIdentifierExpression&>(expression).sql_identifier;
}

size_t SQLIdentifierExpression::_on_hash() const {
  return std::hash<SQLIdentifier>{}(sql_identifier);
}

}  // namespace opossum
