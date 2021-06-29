#pragma once

#include "operators/abstract_read_write_operator.hpp"

namespace opossum {

class CreateTableKeyConstraint : public AbstractReadWriteOperator {
 public:
  CreateTableKeyConstraint(std::unordered_set<ColumnID) init_columns, KeyConstraintType init_key_type,
};
}
