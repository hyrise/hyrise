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

std::ostream& operator<<(std::ostream& stream, const LQPUniqueConstraint& unique_constraint) {
  stream << "{";
  auto expressions_vector = std::vector<std::shared_ptr<AbstractExpression>>{unique_constraint.expressions.begin(),
                                                                             unique_constraint.expressions.end()};
  stream << expressions_vector.at(0)->as_column_name();
  for (auto expression_idx = size_t{1}; expression_idx < expressions_vector.size(); ++expression_idx) {
    stream << ", " << expressions_vector[expression_idx]->as_column_name();
  }
  stream << "}";

  return stream;
}

}  // namespace opossum

namespace std {

size_t hash<opossum::LQPUniqueConstraint>::operator()(const opossum::LQPUniqueConstraint& lqp_unique_constraint) const {
  return lqp_unique_constraint.hash();
}

}  // namespace std
