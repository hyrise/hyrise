#pragma once

#include <memory>
#include <unordered_map>
#include <optional>
#include <vector>

#include "sql_identifier.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

class AbstractExpression;

struct SQLIdentifierContextEntry final {
  std::shared_ptr<AbstractExpression> expression;
  std::optional<SQLIdentifier> identifier;
};

/**
 * Provides a SQLIdentifier -> Expression lookup.
 *
 * Its main purpose is name resolution during the SQL translation. As such,
 * it performs resolution of Table and Column Aliases.
 */
class SQLIdentifierContext final {
 public:
  void set_column_name(const std::shared_ptr<AbstractExpression>& expression, const std::string& column_name);
  void set_table_name(const std::shared_ptr<AbstractExpression>& expression, const std::string& table_name);

  std::shared_ptr<AbstractExpression> resolve_identifier_relaxed(const SQLIdentifier& identifier) const;
  const std::optional<SQLIdentifier> get_expression_identifier(const std::shared_ptr<AbstractExpression>& expression) const;

 private:
  SQLIdentifierContextEntry& _find_or_create_expression_entry(const std::shared_ptr<AbstractExpression>& expression);

  std::vector<SQLIdentifierContextEntry> _entries;
};

}  // namespace
