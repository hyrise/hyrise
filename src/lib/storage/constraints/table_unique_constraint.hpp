#pragma once

#include "table_key_constraint.hpp"

namespace opossum {

/**
 * Container to define a unique constraint for a given set of column ids.
 * Validity checks take place when unique constraints are added to tables. For example, checks for nullability in
 * case of PRIMARY_KEY constraints.
 */
class TableUniqueConstraint final : public TableKeyConstraint {
 public:
  /**
   * Creates a unique constraint based on a given column set @param init_columns and a given @param init_key_type which
   * can be either UNIQUE or PRIMARY_KEY.
   */
  TableUniqueConstraint(std::unordered_set<ColumnID> init_columns, KeyConstraintType init_key_type);
};

using TableUniqueConstraints = std::vector<TableUniqueConstraint>;

}  // namespace opossum
