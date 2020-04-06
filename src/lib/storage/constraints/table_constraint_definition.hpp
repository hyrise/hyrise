#pragma once

#include <vector>

#include "types.hpp"

namespace opossum {

enum class IsPrimaryKey : bool { Yes = true, No = false };

// Defines a unique constraint on a set of column ids. Can optionally be a PRIMARY KEY, requiring the column(s) to be
// non-NULL. For tables, constraints are currently NOT enforced.

struct TableConstraintDefinition final {
  TableConstraintDefinition() = default;

  TableConstraintDefinition(std::unordered_set<ColumnID> column_ids,
                            const IsPrimaryKey init_is_primary_key = IsPrimaryKey::No)
      : columns(std::move(column_ids)), is_primary_key(init_is_primary_key) {}

  bool operator==(const TableConstraintDefinition& rhs) const {
    return columns == rhs.columns && is_primary_key == rhs.is_primary_key;
  }
  bool operator!=(const TableConstraintDefinition& rhs) const { return !(rhs == *this); }

  std::unordered_set<ColumnID> columns;
  IsPrimaryKey is_primary_key;
};

using TableConstraintDefinitions = std::unordered_set<TableConstraintDefinition>;

}  // namespace opossum

namespace std {

template <>
struct hash<opossum::TableConstraintDefinition> {
  size_t operator()(const opossum::TableConstraintDefinition& table_constraint) const {
    auto hash = boost::hash_value(table_constraint.is_primary_key);
    for (const auto& column_id : table_constraint.columns) {
      boost::hash_combine(hash, boost::hash_value(column_id.t));
    }
    return hash;
  }
};

}  // namespace std
