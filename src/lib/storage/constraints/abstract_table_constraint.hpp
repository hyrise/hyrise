#pragma once

#include <unordered_set>

#include "types.hpp"

namespace opossum {

/**
 * Abstract container class for the definition of table constraints based on a set of column ids.
 * Subclasses should leverage the OOP structure to add additional fields. In case of CHECK and FOREIGN KEY constraint
 * implementations, these fields may include check definitions and referenced keys.
 */
class AbstractTableConstraint {
 public:
  explicit AbstractTableConstraint(std::unordered_set<ColumnID> init_columns);
  virtual ~AbstractTableConstraint() = default;

  const std::unordered_set<ColumnID>& columns() const;

  bool operator==(const AbstractTableConstraint& rhs) const;
  bool operator!=(const AbstractTableConstraint& rhs) const;

 protected:
  /**
   * Compare two table constraints of the same type. Only additional fields have to be compared since column ids are
   * already compared by the caller.
   */
  virtual bool _on_shallow_equals(const AbstractTableConstraint& table_constraint) const = 0;

 private:
  std::unordered_set<ColumnID> _columns;
};

}  // namespace opossum
