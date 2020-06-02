#pragma once

#include <unordered_set>

#include "types.hpp"

namespace opossum {

/**
 * Currently, only UNIQUE and PRIMARY_KEY key types are implemented.
 * In the future, we may implement a FOREIGN KEY type and a corresponding subclass as well.
 */
enum class KeyConstraintType { PRIMARY_KEY, UNIQUE };

/**
 * Abstract container class for defining table constraints based on a set of column IDs representing an SQL key.
 */
class TableKeyConstraint {
 public:
  explicit TableKeyConstraint(std::unordered_set<ColumnID> init_columns, KeyConstraintType init_key_type);
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
