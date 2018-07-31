#include "sql_identifier.hpp"

#include <sstream>

#include "boost/functional/hash.hpp"

namespace opossum {

SQLIdentifier::SQLIdentifier(const std::string& column_name, const std::optional<std::string>& table_name)
    : column_name(column_name), table_name(table_name) {}

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

namespace std {

size_t hash<opossum::SQLIdentifier>::operator()(const opossum::SQLIdentifier& sql_identifier) const {
  auto hash = std::hash<std::string>{}(sql_identifier.column_name);

  if (sql_identifier.table_name) {
    boost::hash_combine(hash, *sql_identifier.table_name);
  } else {
    boost::hash_combine(hash, false);
  }

  return hash;
}

}  // namespace std
