#pragma once

#include <tbb/concurrent_vector.h>

#include "abstract_table_constraint.hpp"
#include "storage/table_column_id.hpp"

namespace opossum {

/**
 * Container class to define inclusion constraints for tables.
 */
class TableInclusionConstraint final : public AbstractTableConstraint {
 public:
  TableInclusionConstraint(const std::vector<TableColumnID>& init_determinants, std::vector<ColumnID> init_dependents);

  const std::vector<TableColumnID>& determinants() const;
  const std::vector<ColumnID>& dependents() const;

 protected:
  bool _on_equals(const AbstractTableConstraint& table_constraint) const override;

 private:
  std::vector<TableColumnID> _determinants;
  std::vector<ColumnID> _dependents;
};

std::ostream& operator<<(std::ostream& stream, const TableInclusionConstraint& expression);

using TableInclusionConstraints = tbb::concurrent_vector<TableInclusionConstraint>;

}  // namespace opossum
