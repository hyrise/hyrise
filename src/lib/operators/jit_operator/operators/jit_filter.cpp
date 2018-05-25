#include "jit_filter.hpp"

namespace opossum {

JitFilter::JitFilter(const JitTupleValue& condition) : _condition{condition} {
  DebugAssert(condition.data_type() == DataType::Bool, "Filter condition must be a boolean");
}

std::string JitFilter::description() const { return "[Filter] on x" + std::to_string(_condition.tuple_index()); }

JitTupleValue JitFilter::condition() { return _condition; }

void JitFilter::_consume(JitRuntimeContext& context) const {
  if (!_condition.is_null(context) && _condition.get<bool>(context)) {
    _emit(context);
  }
}

}  // namespace opossum
