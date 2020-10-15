#include "value_expression_id_allocator.hpp"

namespace opossum {

ValueExpressionID ValueExpressionIDAllocator::allocate() { return static_cast<ValueExpressionID>(_value_expression_id_counter++); }

}  // namespace opossum
