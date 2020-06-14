#include "constraints/lqp_unique_constraint.hpp"

namespace opossum {

LQPUniqueConstraint::LQPUniqueConstraint(ExpressionUnorderedSet init_column_expressions)
                                                : column_expressions(std::move(init_column_expressions)) {
  DebugAssert(!init_column_expressions.empty(), "LQPUniqueConstraint cannot be empty");
}

bool LQPUniqueConstraint::operator==(const LQPUniqueConstraint& rhs) const {
  if (column_expressions.size() != rhs.column_expressions.size()) return false;
  return std::all_of(column_expressions.cbegin(), column_expressions.cend(), [&rhs](const auto column_expression) {
    return rhs.column_expressions.contains(column_expression);
  });
}

bool LQPUniqueConstraint::operator!=(const LQPUniqueConstraint& rhs) const { return !(rhs == *this); }

}  // namespace opossum
