#pragma once

#include "types.hpp"

namespace opossum {

/**
 * Allocates ValueIDs for ValueExpressions during SQL translation
 */
class ValueExpressionIDAllocator {
 public:
  ValueExpressionID allocate();

 private:
  ValueExpressionID _value_expression_id_counter{0};
};

}  // namespace opossum
