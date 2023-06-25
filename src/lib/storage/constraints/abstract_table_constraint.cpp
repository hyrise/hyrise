#include "abstract_table_constraint.hpp"

namespace hyrise {

bool AbstractTableConstraint::operator==(const AbstractTableConstraint& rhs) const {
  if (this == &rhs) {
    return true;
  }

  if (typeid(*this) != typeid(rhs)) {
    return false;
  }

  return _on_equals(rhs);
}

bool AbstractTableConstraint::operator!=(const AbstractTableConstraint& rhs) const {
  return !(rhs == *this);
}

}  // namespace hyrise
