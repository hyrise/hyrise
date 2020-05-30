#include "table_unique_constraint.hpp"

namespace opossum {

TableUniqueConstraint::TableUniqueConstraint(const std::unordered_set<ColumnID> &init_columns, KeyConstraintType
                                                                                              init_key_type)
: TableKeyConstraint(init_columns, init_key_type) {
  Assert(init_key_type == KeyConstraintType::UNIQUE || init_key_type == KeyConstraintType::PRIMARY_KEY,
         "Invalid key type: Use UNIQUE or PRIMARY KEY to define unique constraints.");
}

} // namespace opossum