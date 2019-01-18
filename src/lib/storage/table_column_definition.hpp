#pragma once

#include "constant_mappings.hpp"

namespace opossum {

struct TableColumnDefinition final {
  TableColumnDefinition() = default;
  TableColumnDefinition(const std::string& name, const DataType data_type, const bool nullable = false);

  bool operator==(const TableColumnDefinition& rhs) const;

  std::string name;
  DataType data_type{DataType::Int};
  bool nullable{false};
};

// So that google test, e.g., prints readable error messages
std::ostream& operator<<(std::ostream& stream, const TableColumnDefinition& definition);

using TableColumnDefinitions = std::vector<TableColumnDefinition>;

TableColumnDefinitions concatenated(const TableColumnDefinitions& lhs, const TableColumnDefinitions& rhs);

}  // namespace opossum
