#pragma once

#include <vector>

#include "types.hpp"

namespace opossum {

struct TableConstraintDefinition final {
  TableConstraintDefinition(std::vector<ColumnID> column_ids, bool is_primary_key)
      : columns(std::move(column_ids)), is_primary_key(is_primary_key) {}

  std::vector<ColumnID> columns;
  bool is_primary_key;
};

}  // namespace opossum
