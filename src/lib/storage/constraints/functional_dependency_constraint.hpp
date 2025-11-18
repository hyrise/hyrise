#pragma once

#include <unordered_set>
#include <vector>

#include <oneapi/tbb/concurrent_unordered_set.h>  // NOLINT(build/include_order): cpplint identifies TBB as C system headers.s

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
  TableFunctionalDependencyConstraint(std::vector<ColumnID>&& dependant_columns,
                                      std::vector<ColumnID>&& determined_columns);
  TableFunctionalDependencyConstraint() = delete;

  const std::vector<ColumnID>& determinant_columns() const;

  const std::vector<ColumnID>& dependent_columns() const;

  size_t hash() const override;

 protected:
  bool _on_equals(const AbstractTableConstraint& table_constraint) const override;

  std::vector<ColumnID> _determinant_columns;
  std::vector<ColumnID> _dependent_columns;
};

using TableFunctionalDependencyConstraints = tbb::concurrent_unordered_set<TableFunctionalDependencyConstraint>;

}  // namespace hyrise

namespace std {

template <>
struct hash<hyrise::TableFunctionalDependencyConstraint> {
  size_t operator()(const hyrise::TableFunctionalDependencyConstraint& table_functional_dependency_constraint) const;
};

}  // namespace std