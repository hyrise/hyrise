#include "table_constraint_definition.hpp"

namespace opossum {

TableConstraintDefinition::TableConstraintDefinition(std::unordered_set<ColumnID> init_columns,
                                                     std::unordered_set<ConstraintType> init_types)
  : columns(std::move(init_columns)), constraint_types(std::move(init_types)) { validate_input(); }

bool TableConstraintDefinition::operator==(const TableConstraintDefinition& rhs) const {
  return columns == rhs.columns && constraint_types == rhs.constraint_types;
}

bool TableConstraintDefinition::operator!=(const TableConstraintDefinition& rhs) const { return !(rhs == *this); }

const std::unordered_set<ColumnID>& TableConstraintDefinition::get_columns() const { return columns; }

const std::unordered_set<ConstraintType>& TableConstraintDefinition::get_constraint_types() const {
  return constraint_types;
}

bool TableConstraintDefinition::defines(ConstraintType constraint_type) const {
  return constraint_types.contains(constraint_type);
}

void TableConstraintDefinition::add_constraint_type(ConstraintType constraint_type) {
  const auto [iter, success] = constraint_types.insert(constraint_type);
  if(success) {
    validate_input();
  }
}

void TableConstraintDefinition::validate_input() {
  if(constraint_types.contains(ConstraintType::PRIMARY_KEY) {
    Assert(!constraint_types.contains(ConstraintType::UNIQUE), "A UNIQUE constraint is superfluous since "
                 "a PRIMARY_KEY constraint is already defined.");
  }
}

}  // namespace opossum