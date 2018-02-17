#pragma once

#include <string>
#include <vector>

#include "all_type_variant.hpp"

namespace opossum {

struct TableColumnDefinition final {
  TableColumnDefinition(const std::string& name, const DataType data_type, const bool nullable = false);

  std::string name;
  DataType data_type;
  bool nullable;
};

struct TableLayout final {
  TableType table_type{TableType::Data};
  std::vector<TableColumnDefinition> column_definitions;
};

}  // namespace opossum
