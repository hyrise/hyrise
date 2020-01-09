#pragma once

#include <vector>

#include "types.hpp"

namespace opossum {

enum class IsPrimaryKey : bool { Yes = true, No = false };

// Defines a unique constraint on a set of column ids. Can optionally be a PRIMARY KEY, requiring the column(s) to be non-NULL.
// For tables, constraints are currently NOT enforced.

struct TableConstraintDefinition final {
  TableConstraintDefinition(std::unordered_set<ColumnID> column_ids, const IsPrimaryKey is_primary_key)
      : columns(std::move(column_ids)), is_primary_key(is_primary_key) {
  }

  [[nodiscard]] bool equals(const TableConstraintDefinition& other_constraint) const {
    if (is_primary_key != other_constraint.is_primary_key) return false;
    if (columns.size() != other_constraint.columns.size()) return false;

    for (const auto& column_id : columns) {
      if (!other_constraint.columns.contains(column_id)) return false;
    }

    return true;
  }

  std::unordered_set<ColumnID> columns;
  IsPrimaryKey is_primary_key;
};

using TableConstraintDefinitions = std::vector<TableConstraintDefinition>;

}  // namespace opossum
