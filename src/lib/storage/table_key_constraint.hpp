#pragma once

#include "abstract_table_constraint.hpp"

namespace hyrise {

enum class KeyConstraintType { PRIMARY_KEY, UNIQUE };

/**
 * Container class to define uniqueness constraints for tables.
 * As defined by SQL, two types of keys are supported: PRIMARY KEY and UNIQUE keys.
 */
class TableKeyConstraint final : public AbstractTableConstraint {
 public:
  TableKeyConstraint(std::set<ColumnID> init_columns, KeyConstraintType init_key_type);

  KeyConstraintType key_type() const;

  size_t hash() const override;

  /**
   * Required for storing TableKeyConstraints in a sort-based std::set.
   * The comparison result does not need to be meaningful as long as it is consistent.
   */
  bool operator<(const TableKeyConstraint& rhs) const;

 protected:
  bool _on_equals(const AbstractTableConstraint& table_constraint) const override;

 private:
  KeyConstraintType _key_type;
};

/**
 * We use std::set here to have a well-defined iteration order when hashing StaticTableNode.
 */
using TableKeyConstraints = std::set<TableKeyConstraint>;

}  // namespace hyrise
