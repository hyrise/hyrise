#include "jit_filter.hpp"

namespace opossum {

JitFilter::JitFilter(const JitTupleValue& condition) : _condition{condition} {
  DebugAssert(condition.data_type() == JitDataType::Bool, "filter condition mus be of type bool");
}

std::string JitFilter::description() const { return "[Filter] on x" + std::to_string(_condition.tuple_index()); }

void JitFilter::next(JitRuntimeContext& ctx) const {
  if (!_condition.materialize(ctx).is_null() && _condition.materialize(ctx).as<uint8_t>()) {
    emit(ctx);
  }
}

}  // namespace opossum
