#include "table_order_constraint.hpp"

namespace hyrise {

TableOrderConstraint::TableOrderConstraint(std::vector<ColumnID> columns, std::vector<ColumnID> dependent_columns)
    : AbstractTableConstraint{std::move(columns)}, _dependent_columns{std::move(dependent_columns)} {}

const std::vector<ColumnID>& TableOrderConstraint::dependent_columns() const {
  return _dependent_columns;
}

size_t TableOrderConstraint::hash() const {
  auto hash = boost::hash_value(_columns.size());
  boost::hash_combine(hash, _columns);

  boost::hash_combine(hash, _dependent_columns.size());
  boost::hash_combine(hash, _dependent_columns);
  return hash;
}

bool TableOrderConstraint::_on_equals(const AbstractTableConstraint& table_constraint) const {
  DebugAssert(dynamic_cast<const TableOrderConstraint*>(&table_constraint),
              "Different table_constraint type should have been caught by AbstractTableConstraint::operator==");
  return _dependent_columns == static_cast<const TableOrderConstraint&>(table_constraint)._dependent_columns;
}

}  // namespace hyrise
