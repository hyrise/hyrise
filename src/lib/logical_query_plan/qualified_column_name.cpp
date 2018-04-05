#include "qualified_column_name.hpp"

namespace opossum {

QualifiedColumnName::QualifiedColumnName(const std::string& column_name, const std::optional<std::string>& table_name)
: column_name(column_name), table_name(table_name) {}

bool QualifiedColumnName::operator==(const QualifiedColumnName& rhs) const {
  return column_name == rhs.column_name && table_name == rhs.table_name;
}

std::string QualifiedColumnName::as_string() const {
  std::stringstream ss;
  if (table_name) ss << *table_name << ".";
  ss << column_name;
  return ss.str();
}

}  // namespace opossum
