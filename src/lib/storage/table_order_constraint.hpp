#pragma once

#include "abstract_table_constraint.hpp"

namespace opossum {

/**
 * Container class to define order constraints for tables.
 */
class TableOrderConstraint final : public AbstractTableConstraint {
 public:
  TableOrderConstraint(std::vector<ColumnID> init_determinants, std::vector<ColumnID> init_dependents);

  const std::vector<ColumnID>& determinants() const;
  const std::vector<ColumnID>& dependents() const;

 protected:
  bool _on_equals(const AbstractTableConstraint& table_constraint) const override;

 private:
  std::vector<ColumnID> _determinants;
  std::vector<ColumnID> _dependents;
};

using TableOrderConstraints = std::vector<TableOrderConstraint>;

}  // namespace opossum
