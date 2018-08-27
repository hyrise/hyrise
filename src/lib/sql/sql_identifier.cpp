#include "sql_identifier.hpp"

#include <sstream>

namespace opossum {

SQLIdentifier::SQLIdentifier(const std::string& cxlumn_name, const std::optional<std::string>& table_name)
    : cxlumn_name(cxlumn_name), table_name(table_name) {}

bool SQLIdentifier::operator==(const SQLIdentifier& rhs) const {
  return cxlumn_name == rhs.cxlumn_name && table_name == rhs.table_name;
}

std::string SQLIdentifier::as_string() const {
  std::stringstream ss;
  if (table_name) ss << *table_name << ".";
  ss << cxlumn_name;
  return ss.str();
}

}  // namespace opossum
