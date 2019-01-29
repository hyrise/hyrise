#include "jit_limit.hpp"

namespace opossum {

std::string JitLimit::description() const { return "[Limit]"; }

void JitLimit::_consume(JitRuntimeContext& context) const {
  DebugAssert(context.limit_rows > 0, "JitLimit should not be called with limit_rows = 0");
  if (--context.limit_rows == 0) {
    context.chunk_size = 0;
  }
  _emit(context);
}

}  // namespace opossum
