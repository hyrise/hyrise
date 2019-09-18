#pragma once

#include <vector>

#include "types.hpp"

namespace opossum {

// Defines a constraint on a table. Can optionally be a PRIMARY KEY, requiring the column(s) to be non-NULL.
// Constraints are currently NOT ENFORCED.

struct TableConstraintDefinition final {
  TableConstraintDefinition(std::vector<ColumnID> column_ids, bool is_primary_key)
      : columns(std::move(column_ids)), is_primary_key(is_primary_key) {
    DebugAssert(std::is_sorted(column_ids.begin(), column_ids.end()), "Expecting Column IDs to be sorted");
  }

  std::vector<ColumnID> columns;
  bool is_primary_key;
};

}  // namespace opossum
