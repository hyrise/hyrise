#include "jit_limit.hpp"

namespace opossum {

std::string JitLimit::description() const { return "[Limit]"; }

void JitLimit::_consume(JitRuntimeContext& context) const {
  // Function should only be called if context.limit_rows > 0 - no DebugAssert here due to specialization issues
  // Decrement row count for every emitted tuple
  if (--context.limit_rows == 0) {
    // If last row is emitted, set chunk_size to 0 to exit the execution hot loop in the JitReadTuples operator.
    context.chunk_size = 0;
  }
  _emit(context);
}

}  // namespace opossum
