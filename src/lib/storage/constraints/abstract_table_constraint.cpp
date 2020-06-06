#include "abstract_table_constraint.hpp"

namespace opossum {

AbstractTableConstraint::AbstractTableConstraint(std::unordered_set<ColumnID> init_columns)
    : _columns(std::move(init_columns)) {}

const std::unordered_set<ColumnID>& AbstractTableConstraint::columns() const { return _columns; }

bool AbstractTableConstraint::operator!=(const AbstractTableConstraint& rhs) const { return !(rhs == *this); }

}  // namespace opossum
