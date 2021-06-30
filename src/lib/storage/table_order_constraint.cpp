#include "table_order_constraint.hpp"

namespace opossum {

TableOrderConstraint::TableOrderConstraint(std::vector<ColumnID> init_determinants, std::vector<ColumnID> init_dependents)
    : AbstractTableConstraint({}), _determinants(std::move(init_determinants)), _dependents(std::move(init_dependents)) {}

const std::vector<ColumnID>& TableOrderConstraint::determinants() const { return _determinants; }
const std::vector<ColumnID>& TableOrderConstraint::dependents() const { return _dependents; }

bool TableOrderConstraint::_on_equals(const AbstractTableConstraint& table_constraint) const {
  DebugAssert(dynamic_cast<const TableOrderConstraint*>(&table_constraint),
              "Different table_constraint type should have been caught by AbstractTableConstraint::operator==");
  const auto other = static_cast<const TableOrderConstraint&>(table_constraint);
  return _determinants == other.determinants() && _dependents == other.dependents();
}

}  // namespace opossum
