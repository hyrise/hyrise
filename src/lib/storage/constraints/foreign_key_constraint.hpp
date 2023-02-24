#pragma once

#include <unordered_set>

#include "abstract_table_constraint.hpp"

namespace hyrise {

class Table;

/**
 * Container class to define foreign key relationships for tables. Consider the following example when creating a table
 * called nation (roughly taken from TPC-H):
 *
 *     CREATE TABLE nation (n_nationkey int NOT NULL,
                            n_name int NOT NULL,
                            n_regionkey NOT NULL,
                            PRIMARY KEY (n_nationkey),
                            FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey));
 *
 * Given this example, @param foreign_key_columns and @param foreign_key_table reference the table with the foreign key
 * (e.g., nation.r_regionkey) whereas @param primary_key_columns and @param primary_key_table reference the table with
 * the primary key (e.g., region.n_regionkey).
 *
 * Foreign key constraints are translated to inclusion dependencies (INDs) in the LQP.
 */
class ForeignKeyConstraint final : public AbstractTableConstraint {
 public:
  ForeignKeyConstraint(const std::vector<ColumnID>& foreign_key_columns,
                       const std::shared_ptr<Table>& foreign_key_table,
                       const std::vector<ColumnID>& primary_key_columns,
                       const std::shared_ptr<Table>& primary_key_table);
  ForeignKeyConstraint() = delete;

  const std::vector<ColumnID>& foreign_key_columns() const;
  std::shared_ptr<Table> foreign_key_table() const;

  const std::vector<ColumnID>& primary_key_columns() const;
  std::shared_ptr<Table> primary_key_table() const;

  size_t hash() const override;

 protected:
  bool _on_equals(const AbstractTableConstraint& table_constraint) const override;

  std::vector<ColumnID> _foreign_key_columns;
  std::weak_ptr<Table> _foreign_key_table;

  std::vector<ColumnID> _primary_key_columns;
  std::weak_ptr<Table> _primary_key_table;
};

using ForeignKeyConstraints = std::unordered_set<ForeignKeyConstraint>;

}  // namespace hyrise

namespace std {

template <>
struct hash<hyrise::ForeignKeyConstraint> {
  size_t operator()(const hyrise::ForeignKeyConstraint& foreign_key_constraint) const;
};

}  // namespace std
