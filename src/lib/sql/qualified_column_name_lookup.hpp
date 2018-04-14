#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "logical_query_plan/qualified_column_name.hpp"

namespace opossum {

class AbstractExpression;

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
class QualifiedColumnNameLookup final {
 public:
  void add(const QualifiedColumnName& qualified_column_name, const std::shared_ptr<AbstractExpression>& expression);
  void set_table_name(const std::shared_ptr<AbstractExpression>& expression, const std::string& table_name);
  void set_column_name(const std::shared_ptr<AbstractExpression>& expression, const std::string& column_name);

  std::vector<std::shared_ptr<ExpressionLookupEntry>> get(const QualifiedColumnName& qualified_column_name) const;

 private:
  std::unordered_map<std::string, std::vector<std::shared_ptr<ExpressionLookupEntry>>> _entries_by_column_name;
  std::unordered_map<std::shared_ptr<ExpressionLookupEntry>, std::shared_ptr<ExpressionLookupEntry>,
  ExpressionSharedPtrHash,
  ExpressionSharedPtrEquals> _entries_by_expression;
};

}  // namespace
