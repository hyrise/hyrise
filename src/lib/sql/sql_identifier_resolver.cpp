#include "sql_identifier_resolver.hpp"

#include "sql_identifier_resolver_proxy.hpp"
#include "utils/assert.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

void SQLIdentifierResolver::add_column_name(const std::shared_ptr<AbstractExpression>& expression,
                                            const std::string& column_name) {
  auto& entry = _find_or_create_expression_entry(expression);
  entry.identifiers.emplace_back(column_name);
}

void SQLIdentifierResolver::reset_column_names(const std::shared_ptr<opossum::AbstractExpression>& expression) {
  auto& entry = _find_expression_entry(expression);
  entry.identifiers.clear();
}

void SQLIdentifierResolver::set_table_name(const std::shared_ptr<AbstractExpression>& expression,
                                           const std::string& table_name) {
  auto& entry = _find_or_create_expression_entry(expression);
  if (entry.identifiers.empty()) entry.identifiers.emplace_back(expression->as_column_name());

  for (auto& identifier : entry.identifiers) {
    identifier.table_name = table_name;
  }
}

std::shared_ptr<AbstractExpression> SQLIdentifierResolver::resolve_identifier_relaxed(
    const SQLIdentifier& identifier) const {
  std::vector<std::shared_ptr<AbstractExpression>> matching_expressions;
  for (const auto& entry : _entries) {
    for (const auto& entry_identifier : entry.identifiers) {
      if (identifier.table_name) {
        if (identifier.table_name == entry_identifier.table_name &&
            identifier.column_name == entry_identifier.column_name) {
          matching_expressions.emplace_back(entry.expression);
          break;
        }
      } else {
        if (identifier.column_name == entry_identifier.column_name) {
          matching_expressions.emplace_back(entry.expression);
          break;
        }
      }
    }
  }

  if (matching_expressions.size() != 1) return nullptr;  // Identifier is ambiguous/not existing

  return matching_expressions[0];
}

const std::vector<SQLIdentifier>& SQLIdentifierResolver::get_expression_identifiers(
    const std::shared_ptr<AbstractExpression>& expression) const {
  auto entry_iter = std::find_if(_entries.begin(), _entries.end(),
                                 [&](const auto& entry) { return *entry.expression == *expression; });
  const auto empty_vector = std::make_shared<std::vector<SQLIdentifier>>();
  if (entry_iter == _entries.end()) return *empty_vector;
  return entry_iter->identifiers;
}

std::vector<std::shared_ptr<AbstractExpression>> SQLIdentifierResolver::resolve_table_name(
    const std::string& table_name) const {
  std::vector<std::shared_ptr<AbstractExpression>> expressions;
  for (const auto& entry : _entries) {
    for (const auto& identifier : entry.identifiers) {
      if (identifier.table_name == table_name) {
        expressions.emplace_back(entry.expression);
        break;
      }
    }
  }
  return expressions;
}

void SQLIdentifierResolver::append(SQLIdentifierResolver&& rhs) {
  _entries.insert(_entries.end(), rhs._entries.begin(), rhs._entries.end());
}

SQLIdentifierContextEntry& SQLIdentifierResolver::_find_expression_entry(
  const std::shared_ptr<AbstractExpression>& expression) {
    auto entry_iter = std::find_if(_entries.begin(), _entries.end(),
                                   [&](const auto& entry) { return *entry.expression == *expression; });
    Assert(entry_iter != _entries.end(), "The expression does not exist.");
    return *entry_iter;
}

SQLIdentifierContextEntry& SQLIdentifierResolver::_find_or_create_expression_entry(
    const std::shared_ptr<AbstractExpression>& expression) {
  auto entry_iter = std::find_if(_entries.begin(), _entries.end(),
                                 [&](const auto& entry) { return *entry.expression == *expression; });

  // If there is no entry for this Expression, just add one
  if (entry_iter == _entries.end()) {
    SQLIdentifierContextEntry entry{expression, {}};
    entry_iter = _entries.emplace(_entries.end(), entry);
  }

  return *entry_iter;
}

}  // namespace opossum
