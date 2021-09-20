#include "table_inclusion_constraint.hpp"

namespace opossum {

TableInclusionConstraint::TableInclusionConstraint(const std::vector<TableColumnID>& init_determinants, std::vector<ColumnID> init_dependents)
    : AbstractTableConstraint(TableConstraintType::Inclusion, {}),
      _determinants(std::move(init_determinants)),
      _dependents(std::move(init_dependents)) {}

const std::vector<TableColumnID>& TableInclusionConstraint::determinants() const { return _determinants; }
const std::vector<ColumnID>& TableInclusionConstraint::dependents() const { return _dependents; }

bool TableInclusionConstraint::_on_equals(const AbstractTableConstraint& table_constraint) const {
  DebugAssert(dynamic_cast<const TableInclusionConstraint*>(&table_constraint),
              "Different table_constraint type should have been caught by AbstractTableConstraint::operator==");
  const auto other = static_cast<const TableInclusionConstraint&>(table_constraint);
  return _determinants == other.determinants() && _dependents == other.dependents();
}

}  // namespace opossum
