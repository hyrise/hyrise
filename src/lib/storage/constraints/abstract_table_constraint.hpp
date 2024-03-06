#pragma once

#include "types.hpp"

namespace hyrise {

/**
 * Abstract container class for the definition of table constraints. Subclasses should leverage the OOP structure to add
 * additional fields. Besides columns of a stored table, these fields may include check definitions and referenced keys,
 * e.g., for CHECK and FOREIGN KEY constraint implementations.
 *
 * We use table constraints to persist data dependencies. They are not enforced on the table data but describe specific
 * properties of and relationships within data. The main purpose of tracking table constraints is to translate them into
 * data dependencies on the LQP level. Using these data dependencies, we perform dedicated dependency-based query
 * optimization techniques (see optimizer.cpp).
 */
class AbstractTableConstraint {
 public:
  AbstractTableConstraint() = default;
  AbstractTableConstraint(const AbstractTableConstraint&) = default;
  AbstractTableConstraint(AbstractTableConstraint&&) = default;

  virtual ~AbstractTableConstraint() = default;

  AbstractTableConstraint& operator=(const AbstractTableConstraint&) = default;
  AbstractTableConstraint& operator=(AbstractTableConstraint&&) = default;

  bool operator==(const AbstractTableConstraint& rhs) const;
  bool operator!=(const AbstractTableConstraint& rhs) const;

  virtual size_t hash() const = 0;

 protected:
  /**
   * Compare two table constraints of the same type.
   */
  virtual bool _on_equals(const AbstractTableConstraint& table_constraint) const = 0;
};

}  // namespace hyrise
