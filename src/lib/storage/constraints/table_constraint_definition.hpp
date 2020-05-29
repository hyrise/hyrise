#pragma once

#include <unordered_set>

#include "types.hpp"

namespace opossum {

/**
 * Currently, we support UNIQUE and PRIMARY_KEY constraints only.
 * Additional types may be added in the future.
 */
enum class ConstraintType {PRIMARY_KEY, UNIQUE};

/**
 * Container to define constraints (e.g. UNIQUE or PRIMARY KEY) for a given set of column ids.
 * Validity checks take place when constraints are added to actual table objects. For example, nullability-checks
 * in case of PRIMARY_KEY constraints
 */
class TableConstraintDefinition final {
 public:
  TableConstraintDefinition() = delete;
  TableConstraintDefinition(std::unordered_set<ColumnID> init_columns, std::unordered_set<ConstraintType> init_types);

  bool operator==(const TableConstraintDefinition& rhs) const;
  bool operator!=(const TableConstraintDefinition& rhs) const;

  const std::unordered_set<ColumnID>& get_columns() const;
  const std::unordered_set<ConstraintType>& get_constraint_types() const;

  void add_constraint_type(ConstraintType constraint_type);

 private:
  void validate_input();

  std::unordered_set<ColumnID> columns;
  std::unordered_set<ConstraintType> constraint_types;
};

using TableConstraintDefinitions = std::unordered_set<TableConstraintDefinition>;

}  // namespace opossum
