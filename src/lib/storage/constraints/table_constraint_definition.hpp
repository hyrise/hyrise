#pragma once

#include <vector>

#include "types.hpp"

namespace opossum {

enum class IsPrimaryKey : bool { Yes = true, No = false };

// Defines a constraint on a table. Can optionally be a PRIMARY KEY, requiring the column(s) to be non-NULL.
// Constraints are currently NOT ENFORCED.

struct TableConstraintDefinition final {
  TableConstraintDefinition(std::vector<ColumnID> column_ids, const IsPrimaryKey is_primary_key)
      : columns(std::move(column_ids)), is_primary_key(is_primary_key) {
    DebugAssert(std::is_sorted(columns.begin(), columns.end()), "Expecting Column IDs to be sorted");
    Assert(std::unique(columns.begin(), columns.end()) == columns.end(), "Expected Column IDs to be unique");
  }

  std::vector<ColumnID> columns;
  IsPrimaryKey is_primary_key;
};

}  // namespace opossum
