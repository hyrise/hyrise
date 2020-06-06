#include "table_key_constraint.hpp"

namespace opossum {

TableKeyConstraint::TableKeyConstraint(std::unordered_set<ColumnID> init_columns, KeyConstraintType init_key_type)
    : AbstractTableConstraint(std::move(init_columns)), _key_type(init_key_type) {}

KeyConstraintType TableKeyConstraint::key_type() const { return _key_type; }

bool TableKeyConstraint::operator==(const AbstractTableConstraint& rhs) const {
//  const auto& rhs_key_constraint = dynamic_cast<TableKeyConstraint>(rhs);
//  return rhs_key_constraint && key_type() == rhs_key_constraint.key_type() && _columns = rhs_key_constraint.columns();
  return false;
}

}  // namespace opossum
