#pragma once

#include <set>
#include <unordered_set>

#include "abstract_table_constraint.hpp"

namespace hyrise {

enum class KeyConstraintType { PRIMARY_KEY, UNIQUE };

/**
 * Container class to define uniqueness constraints for tables. As defined by SQL, two types of keys are supported:
 * PRIMARY KEY and UNIQUE keys.
 */
class TableKeyConstraint final : public AbstractTableConstraint {
 public:
  /**
   * By requesting a std::set for the constructor, we ensure that the ColumnIDs have a well-defined order when creating
   * the vector (and equal constraints have equal columns). Thus, we can safely hash and compare key constraints without
   * voilating the set semantics of the constraint.
   */
  TableKeyConstraint(const std::set<ColumnID>& columns, KeyConstraintType key_type);
  TableKeyConstraint() = delete;

  KeyConstraintType key_type() const;

  const std::set<ColumnID>& columns() const;

  size_t hash() const override;

  /**
   * Required for storing TableKeyConstraints in a sort-based std::set, which we use for formatted printing of the
   * entire table's unique constraints. The comparison result does not need to be meaningful as long as it is
   * consistent. In fact, we favor primary key constraints to be printed first.
   */
  bool operator<(const TableKeyConstraint& rhs) const;

 protected:
  bool _on_equals(const AbstractTableConstraint& table_constraint) const override;

  KeyConstraintType _key_type;

  /**
   * A std::set orders the columns ascending out of the box, which is desirable when printing them or comparing key
   * constraints.
   */
  std::set<ColumnID> _columns;
};

using TableKeyConstraints = std::unordered_set<TableKeyConstraint>;

}  // namespace hyrise

namespace std {

template <>
struct hash<hyrise::TableKeyConstraint> {
  size_t operator()(const hyrise::TableKeyConstraint& table_constraint) const;
};

}  // namespace std
