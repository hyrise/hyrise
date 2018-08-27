#pragma once

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

struct TableColumnDefinition final {
  TableColumnDefinition() = default;
  TableColumnDefinition(const std::string& name, const DataType data_type, const bool nullable = false);

  bool operator==(const TableColumnDefinition& rhs) const;

  std::string name;
  DataType data_type{DataType::Int};
  bool nullable{false};
};

using TableCxlumnDefinitions = std::vector<TableColumnDefinition>;

TableCxlumnDefinitions concatenated(const TableCxlumnDefinitions& lhs, const TableCxlumnDefinitions& rhs);

}  // namespace opossum
