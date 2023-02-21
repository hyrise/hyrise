#include "abstract_table_constraint.hpp"

namespace hyrise {

AbstractTableConstraint::AbstractTableConstraint(std::vector<ColumnID> columns) : _columns{std::move(columns)} {}

const std::vector<ColumnID>& AbstractTableConstraint::columns() const {
  return _columns;
}

bool AbstractTableConstraint::operator==(const AbstractTableConstraint& rhs) const {
  if (this == &rhs) {
    return true;
  }

  if (typeid(*this) != typeid(rhs) || _columns != rhs._columns) {
    return false;
  }

  return _on_equals(rhs);
}

bool AbstractTableConstraint::operator!=(const AbstractTableConstraint& rhs) const {
  return !(rhs == *this);
}

}  // namespace hyrise
