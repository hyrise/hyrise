#pragma once

#include "abstract_column_expression.hpp"

#include "sql/sql_identifier.hpp"

namespace opossum {

class SQLIdentifierExpression : public AbstractColumnExpression {
 public:
  explicit SQLIdentifierExpression(const SQLIdentifier& sql_identifier);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;

  SQLIdentifier sql_identifier;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
};

}  // namespace opossum
