#pragma once

#include <vector>

#include "types.hpp"

namespace opossum {

struct TableConstraintDefinition final {
  std::vector<ColumnID> columns;
  bool is_primary_key;
};

}  // namespace opossum
