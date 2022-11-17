#pragma once

#include "abstract_table_constraint.hpp"

namespace hyrise {

/**
 * Container class to define order constraints for tables (i.e., ordering a table by @param columns also orders
 * @param dependent_columns).
 */
class TableOrderConstraint final : public AbstractTableConstraint {
 public:
  TableOrderConstraint(std::vector<ColumnID> columns, std::vector<ColumnID> dependent_columns);

  const std::vector<ColumnID>& dependent_columns() const;

  size_t hash() const override;

 protected:
  bool _on_equals(const AbstractTableConstraint& order_constraint) const override;

  std::vector<ColumnID> _dependent_columns;
};

}  // namespace hyrise
