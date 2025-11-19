#pragma once

#include <set>
#include <vector>

#include <oneapi/tbb/concurrent_set.h>  // NOLINT(build/include_order): cpplint identifies TBB as C system headers.s

#include "abstract_table_constraint.hpp"

namespace hyrise {

/**
 * Container class to define functional dependency constraints for tables (i.e., 
 * TODO: add comment here.
 *
 * Table functional dependency constraints are translated to order dependencies (ODs) in the LQP.
 */
class TableFunctionalDependencyConstraint final : public AbstractTableConstraint {
 public:
  TableFunctionalDependencyConstraint(std::set<ColumnID>&& dependant_columns, std::set<ColumnID>&& determined_columns);
  TableFunctionalDependencyConstraint() = delete;

  const std::set<ColumnID>& determinant_columns() const;

  const std::set<ColumnID>& dependent_columns() const;

  size_t hash() const override;

  bool operator<(const TableFunctionalDependencyConstraint& other) const {
    if (determinant_columns() < other.determinant_columns())
      return true;
    if (other.determinant_columns() < determinant_columns())
      return false;
    return dependent_columns() < other.dependent_columns();
  }

 protected:
  bool _on_equals(const AbstractTableConstraint& table_constraint) const override;

  std::set<ColumnID> _determinant_columns;
  std::set<ColumnID> _dependent_columns;
};

using TableFunctionalDependencyConstraints = tbb::concurrent_set<TableFunctionalDependencyConstraint>;

}  // namespace hyrise

namespace std {

template <>
struct hash<hyrise::TableFunctionalDependencyConstraint> {
  size_t operator()(const hyrise::TableFunctionalDependencyConstraint& table_functional_dependency_constraint) const;
};

}  // namespace std