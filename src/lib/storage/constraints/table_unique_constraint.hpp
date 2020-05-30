#pragma once

#include "table_key_constraint.hpp"

namespace opossum {

/**
 * Container to define a unique constraint for a given set of column ids.
 * Validity checks take place when unique constraints are added to tables. For example, checks for nullability in
 * case of PRIMARY_KEY constraints.
 */
class TableUniqueConstraint final : public TableKeyConstraint {
  TableUniqueConstraint(std::unordered_set<ColumnID>& init_columns, KeyConstraintType init_key_type);
};

using TableUniqueConstraints = std::unordered_set<TableUniqueConstraint>;

}  // namespace opossum
