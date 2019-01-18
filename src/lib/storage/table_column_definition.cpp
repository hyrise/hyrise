#include "table_column_definition.hpp"

#include <sstream>

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

std::ostream& operator<<(std::ostream& stream, const TableColumnDefinition& definition) {
  stream << definition.name << " ";
  stream << data_type_to_string.left_at(definition.data_type) << " ";
  stream << (definition.nullable ? "nullable" : "not nullable");
  return stream;
}

}  // namespace opossum
