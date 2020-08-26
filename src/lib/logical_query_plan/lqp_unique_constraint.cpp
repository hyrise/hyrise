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

size_t LQPUniqueConstraint::hash() const {
  size_t hash = 0;
  for (const auto& expression : expressions) {
    // To make the hash independent of the expressions' order, we have to use a commutative operator like XOR.
    hash = hash ^ expression->hash();
  }

  return boost::hash_value(hash - expressions.size());
}

}  // namespace opossum

namespace std {

size_t hash<opossum::LQPUniqueConstraint>::operator()(const opossum::LQPUniqueConstraint& lqp_unique_constraint) const {
  return lqp_unique_constraint.hash();
}

}  // namespace std
