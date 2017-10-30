#include "ast_types.hpp"

#include <sstream>

namespace opossum {

std::string NamedColumnReference::as_string() const {
  std::stringstream ss;
  if (table_name) ss << *table_name << ".";
  ss << column_name;
  return ss.str();
}
}