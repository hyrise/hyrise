#pragma once

#include <vector>

#include "types.hpp"

namespace opossum {

enum class IsPrimaryKey : bool { Yes = true, No = false };

// Defines a unique constraint on a set of column ids. Can optionally be a PRIMARY KEY, requiring the column(s) to be non-NULL.
// For tables, constraints are currently NOT enforced.

struct TableConstraintDefinition final {
  TableConstraintDefinition() = default;

  TableConstraintDefinition(std::unordered_set<ColumnID> column_ids, const IsPrimaryKey is_primary_key)
      : columns(std::move(column_ids)), is_primary_key(is_primary_key) {}

  bool operator==(const TableConstraintDefinition& rhs) const {
    return columns == rhs.columns && is_primary_key == rhs.is_primary_key;
  }
  bool operator!=(const TableConstraintDefinition& rhs) const { return !(rhs == *this); }

  size_t hash() const {
    auto hash = boost::hash_value(is_primary_key);
    for (const auto& column_id : columns) {
      boost::hash_combine(hash, boost::hash_value(column_id.t));
    }
    return hash;
  }

  std::unordered_set<ColumnID> columns;
  IsPrimaryKey is_primary_key;
};

// Wrapper-structs for enabling hash based containers containing TableConstraintDefinition
struct TableConstraintDefinitionHash final {
  size_t operator()(const TableConstraintDefinition& table_constraint_definition) const {
    return table_constraint_definition.hash();
  }
};
struct TableConstraintDefinitionEqual final {
  size_t operator()(const TableConstraintDefinition& constraint_a,
                    const TableConstraintDefinition& constraint_b) const {
    return constraint_a == constraint_b;
  }
};

using TableConstraintDefinitions =
    std::unordered_set<TableConstraintDefinition, TableConstraintDefinitionHash, TableConstraintDefinitionEqual>;

}  // namespace opossum
