#pragma once

#include "expression/abstract_expression.hpp"
#include "types.hpp"

namespace opossum {

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

// Hash function required by Boost.ContainerHash
inline std::size_t hash_value(const LQPUniqueConstraint& unique_constraint) {
  return unique_constraint.hash();
}

using LQPUniqueConstraints = std::vector<LQPUniqueConstraint>;
using LQPUniqueConstraintUnorderedSet = std::unordered_set<LQPUniqueConstraint, boost::hash<LQPUniqueConstraint>>;

}  // namespace opossum


