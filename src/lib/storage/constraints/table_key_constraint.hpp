#pragma once

#include <unordered_set>
#include <set>

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
  TableKeyConstraint(const std::set<ColumnID>& columns, KeyConstraintType init_key_type);

  KeyConstraintType key_type() const;

  size_t hash() const override;

  /**
   * Required for storing TableKeyConstraints in a sort-based std::set, which we used for formatted printing of all
   * table's unique constraints. The comparison result does not need to be meaningful as long as it is consistent.
   */
  bool operator<(const TableKeyConstraint& rhs) const;

 protected:
  bool _on_equals(const AbstractTableConstraint& table_constraint) const override;

 private:
  KeyConstraintType _key_type;
};

using TableKeyConstraints = std::unordered_set<TableKeyConstraint>;

}  // namespace hyrise

namespace std {

template <>
struct hash<hyrise::TableKeyConstraint> {
  size_t operator()(const hyrise::TableKeyConstraint& table_constraint) const;
};

}  // namespace std
