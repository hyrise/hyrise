#include "jit_filter.hpp"

namespace opossum {

JitFilter::JitFilter(const JitTupleValue& condition) : _condition{condition} {
  DebugAssert(condition.data_type() == JitDataType::Bool, "filter condition must be a boolean");
}

std::string JitFilter::description() const { return "[Filter] on x" + std::to_string(_condition.tuple_index()); }

void JitFilter::next(JitRuntimeContext& ctx) const {
  const auto condition_value = _condition.materialize(ctx);
  if (!condition_value.is_null() && condition_value.as<uint8_t>()) {
    emit(ctx);
  }
}

}  // namespace opossum
