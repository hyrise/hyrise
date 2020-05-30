#pragma once

#include <unordered_set>

#include "types.hpp"

namespace opossum {

/**
 * In SQL a key Currently, we support UNIQUE and PRIMARY_KEY types only.
 * In the future, we may add a FOREIGN KEY type as well.
 */
enum class KeyConstraintType {
  PRIMARY_KEY,
  UNIQUE,
  NONE
};

/**
 * Abstract container class to define table constraints, spanning a set of column ids.
 */
class TableKeyConstraint {
 public:
  explicit TableKeyConstraint(const std::unordered_set<ColumnID>& init_columns, KeyConstraintType init_key_type);
  virtual ~TableKeyConstraint() = default;

  const std::unordered_set<ColumnID>& columns() const;
  KeyConstraintType type() const;

  bool operator==(const TableKeyConstraint& rhs) const;
  bool operator!=(const TableKeyConstraint& rhs) const;

 private:
  std::unordered_set<ColumnID> _columns;
  KeyConstraintType _key_type;
};

}  // namespace opossum