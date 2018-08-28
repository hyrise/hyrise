#pragma once

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

struct TableCxlumnDefinition final {
  TableCxlumnDefinition() = default;
  TableCxlumnDefinition(const std::string& name, const DataType data_type, const bool nullable = false);

  bool operator==(const TableCxlumnDefinition& rhs) const;

  std::string name;
  DataType data_type{DataType::Int};
  bool nullable{false};
};

using TableCxlumnDefinitions = std::vector<TableCxlumnDefinition>;

TableCxlumnDefinitions concatenated(const TableCxlumnDefinitions& lhs, const TableCxlumnDefinitions& rhs);

}  // namespace opossum
