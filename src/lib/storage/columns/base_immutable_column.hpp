#pragma once

#include "storage/base_column.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseImmutableColumn : public BaseColumn {
 public:
  void append(const AllTypeVariant& val) final { Fail("Column is immutable."); }
};

}  // namespace opossum
