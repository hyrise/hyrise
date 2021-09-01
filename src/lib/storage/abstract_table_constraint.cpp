#include "abstract_table_constraint.hpp"

namespace opossum {

AbstractTableConstraint::AbstractTableConstraint(const TableConstraintType init_type,
                                                 std::unordered_set<ColumnID> init_columns)
    : _type(init_type), _columns(std::move(init_columns)) {}

const std::unordered_set<ColumnID>& AbstractTableConstraint::columns() const { return _columns; }

bool AbstractTableConstraint::operator==(const AbstractTableConstraint& rhs) const {
  if (this == &rhs) return true;
  if (typeid(*this) != typeid(rhs)) return false;
  if (columns() != rhs.columns()) return false;
  return _on_equals(rhs);
}

TableConstraintType AbstractTableConstraint::type() const { return _type; }

bool AbstractTableConstraint::operator!=(const AbstractTableConstraint& rhs) const { return !(rhs == *this); }

}  // namespace opossum
