#include "abstract_table_constraint.hpp"

namespace opossum {

AbstractTableConstraint::AbstractTableConstraint(std::unordered_set<ColumnID> init_columns)
    : _columns(std::move(init_columns)) {}

const std::unordered_set<ColumnID>& AbstractTableConstraint::columns() const { return _columns; }

bool AbstractTableConstraint::operator==(const AbstractTableConstraint& rhs) const {
  if (this == &rhs) return true;
  if (typeid(*this) != typeid(rhs)) return false;
  if (columns() != rhs.columns()) return false;
  return _on_equals(rhs);
}

bool AbstractTableConstraint::operator!=(const AbstractTableConstraint& rhs) const { return !(rhs == *this); }

}  // namespace opossum
