#pragma once

#include "expression/abstract_expression.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Container structure to define uniqueness for subsets of LQP output expressions. Analogous to SQL's UNIQUE
 * constraint, rows containing NULL values in any of the expressions are always considered to be distinct. For
 * PRIMARY KEY semantics, check if the expressions are nullable, cf. AbstractLQPNode::is_column_nullable.
 */
struct LQPUniqueConstraint final {
  explicit LQPUniqueConstraint(ExpressionUnorderedSet init_expressions);

  bool operator==(const LQPUniqueConstraint& rhs) const;
  bool operator!=(const LQPUniqueConstraint& rhs) const;

  ExpressionUnorderedSet expressions;
};

using LQPUniqueConstraints = std::vector<LQPUniqueConstraint>;

}  // namespace opossum
