#pragma once

#include "types.hpp"

namespace opossum {

struct TableColumnDefinition final {
  TableColumnDefinition() = default;
  TableColumnDefinition(const std::string& name, const DataType data_type, const bool nullable = false);

  std::string name;
  DataType data_type{DataType::Int};
  bool nullable{false};
};

using TableColumnDefinitions = std::vector<TableColumnDefinition>;

}  // namespace opossum
