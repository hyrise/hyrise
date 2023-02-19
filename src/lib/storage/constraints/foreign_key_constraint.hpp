#pragma once

#include <unordered_set>

#include "abstract_table_constraint.hpp"

namespace hyrise {

class Table;

/**
 * Container class to define foreign key relationships for tables. @param table and @param columns reference the table
 * with the key columns (e.g., region.r_regionkey) whereas @param foreign_key_table and @param foreign_key_columns
 * reference the table with the foreign key columns (e.g., nation.n_regionkey).
 */
class ForeignKeyConstraint final : public AbstractTableConstraint {
 public:
  ForeignKeyConstraint(const std::vector<ColumnID>& columns, const std::vector<ColumnID>& foreign_key_columns,
                       const std::shared_ptr<Table>& table, const std::shared_ptr<Table>& foreign_key_table);

  bool operator==(const ForeignKeyConstraint& other) const;
  bool operator!=(const ForeignKeyConstraint& other) const;

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
