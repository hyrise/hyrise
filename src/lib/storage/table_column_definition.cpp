#include "table_column_definition.hpp"

namespace opossum {

TableColumnDefinition::TableColumnDefinition(const std::string& name, const DataType data_type, const bool nullable)
    : name(name), data_type(data_type), nullable(nullable) {}

bool TableColumnDefinition::operator==(const TableColumnDefinition& rhs) const {
  return name == rhs.name && data_type == rhs.data_type && nullable == rhs.nullable;
}

TableCxlumnDefinitions concatenated(const TableCxlumnDefinitions& lhs, const TableCxlumnDefinitions& rhs) {
  auto cxlumn_definitions = lhs;
  cxlumn_definitions.insert(cxlumn_definitions.end(), rhs.begin(), rhs.end());
  return cxlumn_definitions;
}

}  // namespace opossum
