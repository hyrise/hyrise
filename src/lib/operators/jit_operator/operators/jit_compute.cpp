#include "jit_compute.hpp"

#include "jit_expression.hpp"

namespace opossum {

JitCompute::JitCompute(const std::shared_ptr<JitExpression>& expression) : expression{expression} {}

void JitCompute::before_specialization(const Table& in_table) { expression->update_result_type(); }

std::string JitCompute::description() const {
  return "[Compute] x" + std::to_string(expression->result_entry->tuple_index) + " = " + expression->to_string();
}

void JitCompute::_consume(JitRuntimeContext& context) const {
  expression->compute_and_store(context);
  _emit(context);
}

}  // namespace opossum
