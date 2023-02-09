#pragma once

#include "abstract_table_constraint.hpp"

namespace hyrise {

class Table;

/**
 * Container class to define inclusion constraints for tables (e.g., foreign key relationships). Modeled as a constraint
 * of the table with the including columns (e.g., nation.n_natiokey) pointing to the table with the included columns
 * (e.g., customer.c_nationkey).
 */
class ForeignKeyConstraint final : public AbstractTableConstraint {
 public:
  ForeignKeyConstraint(const std::vector<ColumnID>& columns, const std::vector<ColumnID>& foreign_key_columns,
                       const std::shared_ptr<Table>& table, const std::shared_ptr<Table>& foreign_key_table);

  const std::vector<ColumnID>& foreign_key_columns() const;

  const std::shared_ptr<Table> table() const;

  const std::shared_ptr<Table> foreign_key_table() const;

  size_t hash() const override;

 protected:
  bool _on_equals(const AbstractTableConstraint& foreign_key_constraint) const override;

  std::vector<ColumnID> _foreign_key_columns;

  std::weak_ptr<Table> _table;

  std::weak_ptr<Table> _foreign_key_table;
};

using ForeignKeyConstraints = std::unordered_set<ForeignKeyConstraint>;

}  // namespace hyrise

namespace std {

template <>
struct hash<hyrise::ForeignKeyConstraint> {
  size_t operator()(const hyrise::ForeignKeyConstraint& foreign_key_constraint) const;
};

}  // namespace std
