#include "sql_identifier_resolver.hpp"

#include "sql_identifier_resolver_proxy.hpp"
#include "utils/assert.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

void SQLIdentifierResolver::add_column_name(const std::shared_ptr<AbstractExpression>& expression,
                                            const std::string& column_name) {
  auto& entry = _find_or_create_expression_entry(expression);
  if (std::find(entry.column_names.begin(), entry.column_names.end(), column_name) == entry.column_names.end()) {
    // This cannot be implemented as a set because the column_names's order would get lost.
    entry.column_names.emplace_back(column_name);
  }
}

void SQLIdentifierResolver::reset_column_names(const std::shared_ptr<opossum::AbstractExpression>& expression) {
  auto entry_iter = std::find_if(_entries.begin(), _entries.end(),
                                 [&](const auto& entry) { return *entry.expression == *expression; });
  if (entry_iter == _entries.end()) return;
  entry_iter->column_names.clear();
}

void SQLIdentifierResolver::set_table_name(const std::shared_ptr<AbstractExpression>& expression,
                                           const std::string& table_name) {
  auto& entry = _find_or_create_expression_entry(expression);
  entry.table_name = table_name;
}

std::shared_ptr<AbstractExpression> SQLIdentifierResolver::resolve_identifier_relaxed(
    const SQLIdentifier& identifier) const {
  std::vector<std::shared_ptr<AbstractExpression>> matching_expressions;
  for (const auto& entry : _entries) {
    if (identifier.table_name && entry.table_name != identifier.table_name) {
      continue;
    }
    for (const auto& column_name : entry.column_names) {
      if (identifier.column_name == column_name) {
        matching_expressions.emplace_back(entry.expression);
        break;
      }
    }
  }

  if (matching_expressions.size() != 1) return nullptr;  // Identifier is ambiguous/not existing

  return matching_expressions[0];
}

const std::vector<SQLIdentifier> SQLIdentifierResolver::get_expression_identifiers(
    const std::shared_ptr<AbstractExpression>& expression) const {
  auto entry_iter = std::find_if(_entries.begin(), _entries.end(),
                                 [&](const auto& entry) { return *entry.expression == *expression; });

  std::vector<SQLIdentifier> identifiers;
  if (entry_iter == _entries.end()) return identifiers;
  for (const auto& column_name : entry_iter->column_names) {
    identifiers.emplace_back(SQLIdentifier(column_name, entry_iter->table_name));
  }
  return identifiers;
}

std::vector<std::shared_ptr<AbstractExpression>> SQLIdentifierResolver::resolve_table_name(
    const std::string& table_name) const {
  std::vector<std::shared_ptr<AbstractExpression>> expressions;
  for (const auto& entry : _entries) {
    if (entry.table_name == table_name) {
      expressions.emplace_back(entry.expression);
    }
  }
  return expressions;
}

void SQLIdentifierResolver::append(SQLIdentifierResolver&& rhs) {
  _entries.insert(_entries.end(), rhs._entries.begin(), rhs._entries.end());
}

SQLIdentifierContextEntry& SQLIdentifierResolver::_find_or_create_expression_entry(
    const std::shared_ptr<AbstractExpression>& expression) {
  auto entry_iter = std::find_if(_entries.begin(), _entries.end(),
                                 [&](const auto& entry) { return *entry.expression == *expression; });

  // If there is no entry for this Expression, just add one
  if (entry_iter == _entries.end()) {
    SQLIdentifierContextEntry entry{expression, std::nullopt, {}};
    entry_iter = _entries.emplace(_entries.end(), entry);
  }

  return *entry_iter;
}

}  // namespace opossum
