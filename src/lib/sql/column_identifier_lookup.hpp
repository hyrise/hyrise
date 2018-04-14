#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "column_identifier.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

class AbstractExpression;
class ColumnIdentifierLookupProxy;

struct ExpressionLookupEntry final {
  ExpressionLookupEntry(const std::optional<std::string>& table_name, const std::shared_ptr<AbstractExpression>& expression);

  std::optional<std::string> table_name;
  std::shared_ptr<AbstractExpression> expression;
};

/**
 * Provides a QualifiedColumnName -> Expression lookup.
 *
 * Its main purpose is name resolution during the SQL translation. As such,
 * it performs resolution of Table and Column Aliases.
 */
class ColumnIdentifierLookup final {
 public:
  explicit ColumnIdentifierLookup(const std::shared_ptr<ColumnIdentifierLookupProxy>& outer_column_identifier_lookup_proxy);

  void add_expressions(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                       const std::vector<std::string>& column_names);

  void set_table_name(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                      const std::string& table_name);

  void set_column_names(const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                        const std::vector<std::string>& column_names);

  std::shared_ptr<AbstractExpression> resolve_identifier(const ColumnIdentifier& column_identifier) const;

 private:
  std::shared_ptr<ColumnIdentifierLookupProxy> _outer_column_identifier_lookup_proxy;
  std::unordered_map<std::string, std::vector<std::shared_ptr<ExpressionLookupEntry>>> _entries_by_column_name;
  ExpressionUnorderedMap<std::shared_ptr<ExpressionLookupEntry> _entries_by_expression;
};

}  // namespace
