#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "expression/abstract_expression.hpp"
#include "sql_identifier.hpp"

namespace opossum {

class AbstractExpression;

struct SQLIdentifierContextEntry final {
  std::shared_ptr<AbstractExpression> expression;
  std::optional<std::string> table_name;
  std::vector<std::string> column_names;
};

/**
 * Used during SQL translation to obtain the expression an identifier refers to.
 * Manages column/table aliases.
 */
class SQLIdentifierResolver final {
 public:
  /**
   * @{
   * Set/Update/Delete the column/table names of an expression. There can be multiple column names referring to a single
   * expression because a new alias does not replace a former column name or alias.
   */
  void add_column_name(const std::shared_ptr<AbstractExpression>& expression, const std::string& column_name);
  void reset_column_names(const std::shared_ptr<AbstractExpression>& expression);
  void set_table_name(const std::shared_ptr<AbstractExpression>& expression, const std::string& table_name);
  /** @} */

  /**
   * Resolve the expression that an SQLIdentifier refers to.
   * @return    The expression referenced to by @param identifier.
   *            nullptr, if no or multiple such expressions exist
   */
  std::shared_ptr<AbstractExpression> resolve_identifier_relaxed(const SQLIdentifier& identifier) const;

  /**
   * Resolve the identifiers of an @param expression
   * @return    The SQLIdentifiers
   */
  std::vector<SQLIdentifier> get_expression_identifiers(const std::shared_ptr<AbstractExpression>& expression) const;

  /**
   * @return   The column expressions of a table/subquery identified by @param table_name.
   */
  std::vector<std::shared_ptr<AbstractExpression>> resolve_table_name(const std::string& table_name) const;

  /**
   * Move all entries from another resolver @param rhs into this resolver
   */
  void append(SQLIdentifierResolver&& rhs);

 private:
  SQLIdentifierContextEntry& _find_or_create_expression_entry(const std::shared_ptr<AbstractExpression>& expression);

  std::vector<SQLIdentifierContextEntry> _entries;
};

}  // namespace opossum
