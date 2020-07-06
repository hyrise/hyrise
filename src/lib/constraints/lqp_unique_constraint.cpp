#include "constraints/lqp_unique_constraint.hpp"

namespace opossum {

LQPUniqueConstraint::LQPUniqueConstraint(ExpressionUnorderedSet init_expressions)
    : expressions(std::move(init_expressions)) {
  DebugAssert(!expressions.empty(), "LQPUniqueConstraint can not be empty.");
}

bool LQPUniqueConstraint::operator==(const LQPUniqueConstraint& rhs) const {
  if (expressions.size() != rhs.expressions.size()) return false;
  return std::all_of(expressions.cbegin(), expressions.cend(),
                     [&rhs](const auto column_expression) { return rhs.expressions.contains(column_expression); });
}

bool LQPUniqueConstraint::operator!=(const LQPUniqueConstraint& rhs) const { return !(rhs == *this); }

}  // namespace opossum
