#pragma once

#include "abstract_table_constraint.hpp"

namespace hyrise {

/**
 * Container class to define inclusion constraints for tables (e.g., foreign key relationships).
 */
class TableInclusionConstraint final : public AbstractTableConstraint {
 public:
  TableInclusionConstraint(std::vector<ColumnID> columns, std::vector<ColumnID> dependent_columns,
                           const std::string& referenced_table_name);

  const std::vector<ColumnID>& dependent_columns() const;

  const std::string& referenced_table_name() const;

  size_t hash() const override;

 protected:
  bool _on_equals(const AbstractTableConstraint& inclusion_constraint) const override;

  std::vector<ColumnID> _dependent_columns;

  // Table names are the unique identifier of relational tables. Furthermore, we have no other means to actually check
  // if two tables are the same.
  std::string _referenced_table_name;
};

using TableInclusionConstraints = std::unordered_set<TableInclusionConstraint>;

}  // namespace hyrise

namespace std {

template <>
struct hash<hyrise::TableInclusionConstraint> {
  size_t operator()(const hyrise::TableInclusionConstraint& inclusion_constraint) const;
};

}  // namespace std
