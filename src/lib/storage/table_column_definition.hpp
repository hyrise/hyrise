#pragma once

#include "types.hpp"

namespace opossum {

struct TableColumnDefinition final {
  TableColumnDefinition() = default;
  TableColumnDefinition(const std::string& name, const DataType data_type, const bool nullable = false);

  inline bool operator==(const TableColumnDefinition& rhs) const {
    return name == rhs.name && data_type == rhs.data_type && nullable == rhs.nullable;
  }

  std::string name;
  DataType data_type{DataType::Int};
  bool nullable{false};
};

using TableColumnDefinitions = std::vector<TableColumnDefinition>;

inline TableColumnDefinitions concatenated(const TableColumnDefinitions& lhs, const TableColumnDefinitions& rhs) {
  auto column_definitions = lhs;
  column_definitions.insert(column_definitions.end(), rhs.begin(), rhs.end());
  return column_definitions;
}

}  // namespace opossum
