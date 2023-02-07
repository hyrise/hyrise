#pragma once

#include "types.hpp"

namespace hyrise {

/**
 * Abstract container class for the definition of table constraints based on a set of column ids.
 * Subclasses should leverage the OOP structure to add additional fields. In case of CHECK and FOREIGN KEY constraint
 * implementations, these fields may include check definitions and referenced keys.
 */
class AbstractTableConstraint {
 public:
  explicit AbstractTableConstraint(std::vector<ColumnID> columns);

  AbstractTableConstraint() = delete;
  AbstractTableConstraint(const AbstractTableConstraint&) = default;
  AbstractTableConstraint(AbstractTableConstraint&&) = default;
  AbstractTableConstraint& operator=(const AbstractTableConstraint&) = default;
  AbstractTableConstraint& operator=(AbstractTableConstraint&&) = default;

  virtual ~AbstractTableConstraint() = default;

  // Data dependencies might have either set or list semanticss. A list is generally applicable, and a constraint is
  // responsible for handling columns accordingly when it uses certain semantics see table_key_constraint.hpp).
  const std::vector<ColumnID>& columns() const;

  bool operator==(const AbstractTableConstraint& rhs) const;
  bool operator!=(const AbstractTableConstraint& rhs) const;

  virtual size_t hash() const = 0;

 protected:
  /**
   * Compare two table constraints of the same type. Only additional fields have to be compared since column ids are
   * already compared by the caller.
   */
  virtual bool _on_equals(const AbstractTableConstraint& table_constraint) const = 0;

  std::vector<ColumnID> _columns;
};

}  // namespace hyrise
