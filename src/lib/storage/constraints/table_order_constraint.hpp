#pragma once

#include "abstract_table_constraint.hpp"

namespace hyrise {

/**
 * Container class to define order constraints for tables (i.e., ordering a table by @param columns also orders
 * @param dependent_columns).
 */
class TableOrderConstraint final : public AbstractTableConstraint {
 public:
  TableOrderConstraint(std::vector<ColumnID> columns, std::vector<ColumnID> ordered_columns);

  const std::vector<ColumnID>& ordered_columns() const;

  size_t hash() const override;

 protected:
  bool _on_equals(const AbstractTableConstraint& order_constraint) const override;

  std::vector<ColumnID> _ordered_columns;
};

using TableOrderConstraints = std::unordered_set<TableOrderConstraint>;

}  // namespace hyrise

namespace std {

template <>
struct hash<hyrise::TableOrderConstraint> {
  size_t operator()(const hyrise::TableOrderConstraint& order_constraint) const;
};

}  // namespace std
