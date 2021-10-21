#include "table_inclusion_constraint.hpp"

namespace opossum {

TableInclusionConstraint::TableInclusionConstraint(const std::vector<TableColumnID>& init_determinants,
                                                   std::vector<ColumnID> init_dependents)
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

std::ostream& operator<<(std::ostream& stream, const TableInclusionConstraint& expression) {
  stream << "{";
  const auto& determinants = expression.determinants();
  const auto& dependents = expression.dependents();
  stream << determinants.at(0);
  for (auto determinant_idx = size_t{1}; determinant_idx < determinants.size(); ++determinant_idx) {
    stream << ", " << determinants[determinant_idx];
  }

  stream << "} => {";
  stream << dependents.at(0);
  for (auto dependent_idx = size_t{1}; dependent_idx < dependents.size(); ++dependent_idx) {
    stream << ", " << dependents[dependent_idx];
  }
  stream << "}";

  return stream;
}

}  // namespace opossum
