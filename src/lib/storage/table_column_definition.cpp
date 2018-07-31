#include "table_column_definition.hpp"

namespace opossum {

TableColumnDefinition::TableColumnDefinition(const std::string& name, const DataType data_type, const bool nullable)
    : name(name), data_type(data_type), nullable(nullable) {}

bool TableColumnDefinition::operator==(const TableColumnDefinition& rhs) const {
  return name == rhs.name && data_type == rhs.data_type && nullable == rhs.nullable;
}

TableColumnDefinitions concatenated(const TableColumnDefinitions& lhs, const TableColumnDefinitions& rhs) {
  auto column_definitions = lhs;
  column_definitions.insert(column_definitions.end(), rhs.begin(), rhs.end());
  return column_definitions;
}

}  // namespace opossum
