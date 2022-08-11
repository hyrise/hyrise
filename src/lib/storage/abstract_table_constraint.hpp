#pragma once

#include <set>

#include "types.hpp"

namespace hyrise {

/**
 * Abstract container class for the definition of table constraints based on a set of column ids.
 * Subclasses should leverage the OOP structure to add additional fields. In case of CHECK and FOREIGN KEY constraint
 * implementations, these fields may include check definitions and referenced keys.
 */
class AbstractTableConstraint {
 public:
  explicit AbstractTableConstraint(std::set<ColumnID> init_columns);

  AbstractTableConstraint(const AbstractTableConstraint&) = default;
  AbstractTableConstraint(AbstractTableConstraint&&) = default;
  AbstractTableConstraint& operator=(const AbstractTableConstraint&) = default;
  AbstractTableConstraint& operator=(AbstractTableConstraint&&) = default;

  virtual ~AbstractTableConstraint() = default;

  const std::set<ColumnID>& columns() const;

  bool operator==(const AbstractTableConstraint& rhs) const;
  bool operator!=(const AbstractTableConstraint& rhs) const;

  virtual size_t hash() const = 0;

 protected:
  /**
   * Compare two table constraints of the same type. Only additional fields have to be compared since column ids are
   * already compared by the caller.
   */
  virtual bool _on_equals(const AbstractTableConstraint& table_constraint) const = 0;

 private:
  /**
   * Usually, we prefer unordered_set because it ensures lookups in O(1). However, std::set guarantees a well-defined
   * order when iterating it. We use this property for hashing table constraints.
   */
  std::set<ColumnID> _columns;
};

}  // namespace hyrise
