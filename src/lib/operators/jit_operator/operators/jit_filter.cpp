#include "jit_filter.hpp"

#include "jit_expression.hpp"

namespace opossum {

JitFilter::JitFilter(const std::shared_ptr<const JitExpression>& expression) : expression{expression} {
  DebugAssert(expression->result_entry.data_type == DataType::Bool, "Filter condition must be a boolean");
}

std::string JitFilter::description() const { return "[Filter] on x = " + expression->to_string(); }

void JitFilter::_consume(JitRuntimeContext& context) const {
  const auto result = expression->compute<bool>(context);
  if ((!expression->result_entry.is_nullable || result) && result.value()) {
    _emit(context);
  }
}

}  // namespace opossum
