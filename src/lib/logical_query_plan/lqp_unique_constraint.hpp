#pragma once

#include "expression/abstract_expression.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * Container structure to define uniqueness for subsets of LQP output expressions. Analogous to SQL's UNIQUE
 * constraint, rows containing NULL values in any of the expressions are always considered to be distinct. For
 * PRIMARY KEY semantics, check if the expressions are nullable, cf. AbstractLQPNode::is_column_nullable.
 *
 * NOTE: Unique constraints are only valid for LQP nodes that contain no invalidated rows (i.e., where there has
 *       been a ValidateNode before or where MVCC is disabled).
 */
struct LQPUniqueConstraint final {
  explicit LQPUniqueConstraint(ExpressionUnorderedSet init_expressions);

  bool operator==(const LQPUniqueConstraint& rhs) const;
  bool operator!=(const LQPUniqueConstraint& rhs) const;
  size_t hash() const;

  ExpressionUnorderedSet expressions;
};

std::ostream& operator<<(std::ostream& stream, const LQPUniqueConstraint& unique_constraint);

using LQPUniqueConstraints = std::vector<LQPUniqueConstraint>;

}  // namespace hyrise

namespace std {

template <>
struct hash<hyrise::LQPUniqueConstraint> {
  size_t operator()(const hyrise::LQPUniqueConstraint& lqp_unique_constraint) const;
};

}  // namespace std
