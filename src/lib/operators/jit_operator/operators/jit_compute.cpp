#include "jit_compute.hpp"

#include "jit_expression.hpp"

namespace opossum {

JitCompute::JitCompute(const std::shared_ptr<const JitExpression>& expression) : expression{expression} {}

std::string JitCompute::description() const {
  return "[Compute] x" + std::to_string(expression->result_entry.tuple_index) + " = " + expression->to_string();
}

void JitCompute::_consume(JitRuntimeContext& context) const {
  expression->compute_and_store(context);
  _emit(context);
}

}  // namespace opossum
