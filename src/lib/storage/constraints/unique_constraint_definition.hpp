#pragma once

#include <vector>

#include "types.hpp"

namespace opossum {

enum class IsPrimaryKey : bool { Yes = true, No = false };

// Defines a unique constraint on a set of column ids. Can optionally be a PRIMARY KEY, requiring the column(s) to be non-NULL.
// For tables, constraints are currently NOT enforced.

struct UniqueConstraintDefinition final {
  UniqueConstraintDefinition(std::vector<ColumnID> column_ids, const IsPrimaryKey is_primary_key)
      : columns(std::move(column_ids)), is_primary_key(is_primary_key) {
    Assert(std::is_sorted(columns.begin(), columns.end()), "Expecting Column IDs to be sorted");
    Assert(std::unique(columns.begin(), columns.end()) == columns.end(), "Expected Column IDs to be unique");
  }

  [[nodiscard]] bool equals(const UniqueConstraintDefinition& other_constraint) const {
    if(is_primary_key != other_constraint.is_primary_key) return false;
    if(columns.size() != other_constraint.columns.size()) return false;

    // Due to the enforced sorting, we can compare both vectors element-wise
    for(ColumnID i{0}; i < columns.size(); i++) {
      if(columns[i] != other_constraint.columns[i]) return false;
    }

    return true;
  }

  std::vector<ColumnID> columns;
  IsPrimaryKey is_primary_key;
};

using UniqueConstraintDefinitions = std::vector<UniqueConstraintDefinition>;

}  // namespace opossum
