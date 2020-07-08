#pragma once

#include "expression/abstract_expression.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Container structure to define uniqueness for subsets of LQP output expressions.
 * A set of "unique" output expressions means distinct values. However, any number of NULL-values
 * (AbstractLQPNode::is_column_nullable) is allowed, similar to table key constraints with KeyConstraintType::UNIQUE.
 */
struct LQPUniqueConstraint final {
  explicit LQPUniqueConstraint(ExpressionUnorderedSet init_expressions);

  bool operator==(const LQPUniqueConstraint& rhs) const;
  bool operator!=(const LQPUniqueConstraint& rhs) const;

  ExpressionUnorderedSet expressions;
};

using LQPUniqueConstraints = std::vector<LQPUniqueConstraint>;

}  // namespace opossum
