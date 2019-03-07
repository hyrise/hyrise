#include "jit_filter.hpp"

#include "jit_expression.hpp"

namespace opossum {

JitFilter::JitFilter(const std::shared_ptr<const JitExpression>& expression) : _expression{expression} {
  DebugAssert(_expression->result_entry().data_type() == DataType::Bool, "Filter condition must be a boolean");
}

std::string JitFilter::description() const { return "[Filter] on x = " + _expression->to_string(); }

std::shared_ptr<const JitExpression> JitFilter::expression() { return _expression; }

void JitFilter::_consume(JitRuntimeContext& context) const {
  const auto result = _expression->compute<bool>(context);
  if ((!_expression->result_entry().is_nullable() || result.has_value()) && result.value()) {
    _emit(context);
  }
}

}  // namespace opossum
