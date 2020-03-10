#include "sql_identifier.hpp"

#include <sstream>

namespace opossum {

SQLIdentifier::SQLIdentifier(const std::string& init_column_name, const std::optional<std::string>& init_table_name)
    : column_name(init_column_name), table_name(init_table_name) {}

bool SQLIdentifier::operator==(const SQLIdentifier& rhs) const {
  return column_name == rhs.column_name && table_name == rhs.table_name;
}

std::string SQLIdentifier::as_string() const {
  std::stringstream ss;
  if (table_name) ss << *table_name << ".";
  ss << column_name;
  return ss.str();
}

}  // namespace opossum
