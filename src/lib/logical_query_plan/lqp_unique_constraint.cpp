#include "lqp_unique_constraint.hpp"

namespace opossum {

LQPUniqueConstraint::LQPUniqueConstraint(ExpressionUnorderedSet init_expressions)
    : expressions(std::move(init_expressions)) {
  Assert(!expressions.empty(), "LQPUniqueConstraint cannot be empty.");
}

bool LQPUniqueConstraint::operator==(const LQPUniqueConstraint& rhs) const {
  if (expressions.size() != rhs.expressions.size()) return false;
  return std::all_of(expressions.cbegin(), expressions.cend(),
                     [&rhs](const auto column_expression) { return rhs.expressions.contains(column_expression); });
}

bool LQPUniqueConstraint::operator!=(const LQPUniqueConstraint& rhs) const { return !(rhs == *this); }

}  // namespace opossum

namespace std {

size_t hash<opossum::LQPUniqueConstraint>::operator()(const opossum::LQPUniqueConstraint& lqp_unique_constraint) const {
  return boost::hash_range(lqp_unique_constraint.expressions.cbegin(), lqp_unique_constraint.expressions.cend());
}

}  // namespace std
