#include "column_identifier_lookup.hpp"

#include <algorithm>

#include "utils/assert.hpp"

namespace opossum {

ExpressionLookupEntry::ExpressionLookupEntry(const std::optional<std::string>& table_name,
                                             const std::shared_ptr<AbstractExpression>& expression):
  table_name(table_name), expression(expression)
{

}

void ColumnIdentifierLookup::add(const ColumnIdentifier& qualified_column_name,
                           const std::shared_ptr<AbstractExpression>& expression) {
  const auto entry = std::make_shared<ExpressionLookupEntry>(qualified_column_name.table_name, expression);

  const auto entry_added = _entries_by_expression.emplace(expression, entry).second;
  Assert(entry_added, "Expression already in Lookup");

  _entries_by_column_name[qualified_column_name.column_name].emplace_back(entry);
}

void ColumnIdentifierLookup::set_table_name(const std::shared_ptr<AbstractExpression>& expression, const std::string& table_name) {
  const auto entry_iter = _entries_by_expression.find(expression);
  Assert(entry_iter != _entries_by_expression.end(), "Expression not in lookup");
  entry_iter->second->table_name = table_name;
}

void ColumnIdentifierLookup::set_column_name(const std::shared_ptr<AbstractExpression>& expression, const std::string& column_name) {
  const auto entry_iter = _entries_by_expression.find(expression);
  Assert(entry_iter != _entries_by_expression.end(), "Expression not in lookup");
  const auto entry = entry_iter->second;

  const auto entries_with_column_name_iter = _entries_by_column_name.find(column_name);
  Assert(entries_with_column_name_iter != _entries_by_column_name.end(), "Column name not in lookup. Bug in ColumnIdentifierLookup.");

  const auto& entries_with_column_name = entries_with_column_name_iter->second;

  const auto entry_iter2 = std::find_if(entries_with_column_name.begin(), entries_with_column_name.end(), [&](const auto& entry2) {
    return entry->expression->deep_equals(*entry2->expression);
  });
  Assert(entry_iter2 != entries_with_column_name.end(), "Entry not in lookup. Bug in ColumnIdentifierLookup.");

  entries_with_column_name.erase(entry_iter2);
  if (entries_with_column_name.empty()) {
    _entries_by_column_name.erase(entries_with_column_name_iter);
  }

  _entries_by_column_name[column_name].emplace_back(entry);
}

std::vector<std::shared_ptr<ExpressionLookupEntry>> ColumnIdentifierLookup::get(const ColumnIdentifier& qualified_column_name) const {
  const auto entry_iter = _entries_by_column_name.find(qualified_column_name.column_name);
  if (entry_iter == _entries_by_column_name.end()) {
    return {};
  }

  if (!qualified_column_name.table_name) {
    return entry_iter->second;
  }

  std::vector<std::shared_ptr<ExpressionLookupEntry>> entries;
  for (const auto& entry : entry_iter->second) {
    if (entry->table_name == qualified_column_name.table_name) {
      entries.emplace_back(entry);
    }
  }

  return entries;
}

}  // namespace opossum
