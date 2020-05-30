#pragma once

#include <unordered_set>

#include "types.hpp"

namespace opossum {

/**
 * Currently, we support UNIQUE and PRIMARY_KEY types only.
 * In the future, we may add a FOREIGN KEY type as well.
 */
enum KeyConstraintType : bool {PRIMARY_KEY, UNIQUE};

/**
 * Container to define a table key constraint spanning a set of columns.
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