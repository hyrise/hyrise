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

  AbstractTableConstraint(const AbstractTableConstraint&) = default;
  AbstractTableConstraint(AbstractTableConstraint&&) = default;
  AbstractTableConstraint& operator=(const AbstractTableConstraint&) = default;
  AbstractTableConstraint& operator=(AbstractTableConstraint&&) = default;

  virtual ~AbstractTableConstraint() = default;

  // Some data dependencies have set semantics for columns, others have list semantics. A list is generally applicable,
  // and a constraint is responsible for handling columns accordingly when it has set semantics in its own
  // (see table_key_constraint.hpp).
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

 private:
  AbstractTableConstraint() = delete;
};

}  // namespace hyrise
