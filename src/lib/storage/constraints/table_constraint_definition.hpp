#pragma once

#include <vector>

#include "types.hpp"

namespace opossum {

/**
 * Currently, we support UNIQUE and PRIMARY_KEY constraints only.
 * Additional types may be added in the future.
 */
enum class ConstraintType {PrimaryKey, Unique};

/**
 * Container to define constraints for a given set of column ids.st
 * Some logical checks, like for instance nullability-checks, are performed when constraints are added to actual
 * tables.
 */
class TableConstraintDefinition final {
 public:
  TableConstraintDefinition() = delete;

  TableConstraintDefinition(std::unordered_set<ColumnID> init_columns, std::unordered_set<ConstraintType> init_types)
      : columns(std::move(init_columns)), constraint_types(init_types) {


  }

  bool operator==(const TableConstraintDefinition& rhs) const {
    return columns == rhs.columns && constraint_types == rhs.constraint_types;
  }
  bool operator!=(const TableConstraintDefinition& rhs) const { return !(rhs == *this); }

 private:
  std::unordered_set<ColumnID> columns;
  std::unordered_set<ConstraintType> constraint_types;
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
