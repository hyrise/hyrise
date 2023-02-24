#pragma once

#include <unordered_set>

#include "abstract_table_constraint.hpp"

namespace hyrise {

/**
 * Container class to define order constraints for tables (i.e., ordering a table by @param ordering_columns also orders
 * @param ordered_columns). An order constraint must not be mistaken as information about the actual sorting of a table
 * (we use Chunk::individually_sorted_by() to get sortedness information on a chunk level).
 */
class TableOrderConstraint final : public AbstractTableConstraint {
 public:
  TableOrderConstraint(const std::vector<ColumnID>& ordering_columns, const std::vector<ColumnID>& ordered_columns);
  TableOrderConstraint() = delete;

  const std::vector<ColumnID>& ordering_columns() const;

  const std::vector<ColumnID>& ordered_columns() const;

  size_t hash() const override;

 protected:
  bool _on_equals(const AbstractTableConstraint& table_constraint) const override;

  std::vector<ColumnID> _ordering_columns;
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
