#include "table_key_constraint.hpp"

namespace opossum {

TableKeyConstraint::TableKeyConstraint(std::unordered_set<ColumnID> &init_columns, KeyConstraintType init_key_type)
  : _columns(std::move(init_columns)), _key_type(init_key_type) {}

const std::unordered_set<ColumnID>& TableKeyConstraint::columns() const {
  return _columns;
}

const KeyConstraintType TableKeyConstraint::type() const {
  return _type;
}

bool TableKeyConstraint::operator==(const TableKeyConstraint& rhs) const {
  return columns == rhs.columns && key_type == rhs.key_type;
}

bool TableKeyConstraint::operator!=(const TableKeyConstraint& rhs) const { return !(rhs == *this); }

} // namespace opossum
