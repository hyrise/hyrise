#pragma once

#include <unordered_set>

#include "types.hpp"

namespace opossum {

/**
 * Abstract container class for the definition of table constraints which are based on a set of column
 * ids. Subclasses should leverage the OOP structure to add additional fields. In case of CHECK and FOREIGN KEY
 * implementations, these fields may include check definitions and referenced keys.
 */
class AbstractTableConstraint {
 public:
  explicit AbstractTableConstraint(std::unordered_set<ColumnID> init_columns);
  virtual ~AbstractTableConstraint() = default;

  const std::unordered_set<ColumnID>& columns() const;

  virtual bool operator==(const AbstractTableConstraint& rhs) const = 0;
  virtual bool operator!=(const AbstractTableConstraint& rhs) const;

 private:
  std::unordered_set<ColumnID> _columns;
};

}  // namespace opossum
