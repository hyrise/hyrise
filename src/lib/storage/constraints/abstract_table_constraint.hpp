#pragma once

#include <unordered_set>

#include "types.hpp"

namespace opossum {

/**
 * Abstract class for defining table constraints on a given set of column IDs.
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
