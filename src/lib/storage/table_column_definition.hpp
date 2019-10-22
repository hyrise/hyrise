#pragma once

#include "all_type_variant.hpp"
#include "constant_mappings.hpp"
#include "types.hpp"

namespace opossum {

struct TableColumnDefinition final {
  TableColumnDefinition() = default;
  TableColumnDefinition(const std::string& name, const DataType data_type, const bool nullable);

  bool operator==(const TableColumnDefinition& rhs) const;
  size_t hash() const;

  std::string name;
  DataType data_type{DataType::Int};
  bool nullable{false};
};

// So that google test, e.g., prints readable error messages
inline std::ostream& operator<<(std::ostream& stream, const TableColumnDefinition& definition) {
  stream << definition.name << " ";
  stream << definition.data_type << " ";
  stream << (definition.nullable ? "nullable" : "not nullable");
  return stream;
}

using TableColumnDefinitions = std::vector<TableColumnDefinition>;

TableColumnDefinitions concatenated(const TableColumnDefinitions& lhs, const TableColumnDefinitions& rhs);

}  // namespace opossum
