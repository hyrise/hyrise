#include "functional_dependency_constraint.hpp"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <utility>
#include <vector>

#include <boost/container_hash/hash.hpp>

#include "storage/constraints/abstract_table_constraint.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

TableFunctionalDependencyConstraint::TableFunctionalDependencyConstraint(std::set<ColumnID>&& determinant_columns,
                                                                         std::set<ColumnID>&& dependent_columns)
    : AbstractTableConstraint{TableConstraintType::FunctionalDependency},
      _determinant_columns{std::move(determinant_columns)},
      _dependent_columns{std::move(dependent_columns)} {
  Assert(!_determinant_columns.empty(), "Did not expect useless constraint.");
  Assert(!_dependent_columns.empty(), "Constant columns are currently not considered.");
  if constexpr (HYRISE_DEBUG) {
    for (const auto column : _dependent_columns) {
      Assert(std::ranges::find(_determinant_columns, column) == _determinant_columns.end(),
             "Left hand side and right hand side columns must be disjoint");
    }
  }
}

const std::set<ColumnID>& TableFunctionalDependencyConstraint::dependent_columns() const {
  return _dependent_columns;
}

const std::set<ColumnID>& TableFunctionalDependencyConstraint::determinant_columns() const {
  return _determinant_columns;
}

size_t TableFunctionalDependencyConstraint::hash() const {
  auto hash = size_t{0};
  boost::hash_combine(hash, _dependent_columns);
  boost::hash_combine(hash, _determinant_columns);
  return hash;
}

bool TableFunctionalDependencyConstraint::_on_equals(const AbstractTableConstraint& table_constraint) const {
  DebugAssert(dynamic_cast<const TableFunctionalDependencyConstraint*>(&table_constraint),
              "Different table_constraint type should have been caught by AbstractTableConstraint::operator==");
  const auto& rhs = static_cast<const TableFunctionalDependencyConstraint&>(table_constraint);
  return _dependent_columns == rhs._dependent_columns && _determinant_columns == rhs._determinant_columns;
}

}  // namespace hyrise

namespace std {

size_t hash<hyrise::TableFunctionalDependencyConstraint>::operator()(
    const hyrise::TableFunctionalDependencyConstraint& table_functional_dependency_constraint) const {
  return table_functional_dependency_constraint.hash();
}

}  // namespace std
