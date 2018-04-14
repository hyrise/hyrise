#include "column_identifier.hpp"

namespace opossum {

ColumnIdentifier::ColumnIdentifier(const std::string& column_name, const std::optional<std::string>& table_name)
: column_name(column_name), table_name(table_name) {}

bool ColumnIdentifier::operator==(const ColumnIdentifier& rhs) const {
  return column_name == rhs.column_name && table_name == rhs.table_name;
}

std::string ColumnIdentifier::as_string() const {
  std::stringstream ss;
  if (table_name) ss << *table_name << ".";
  ss << column_name;
  return ss.str();
}

}  // namespace opossum
