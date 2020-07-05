#pragma once

#include "expression/abstract_expression.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Container structure to define uniqueness for subsets of LQP output expressions.
 * In the current implementation, "unique" means distinct values in a given subset of LQP output expressions.
 * However, any number of NULL-values is allowed, similar to table constraints with KeyConstraintType::UNIQUE.
 */
struct LQPUniqueConstraint final {
  explicit LQPUniqueConstraint(ExpressionUnorderedSet init_expressions);

  bool operator==(const LQPUniqueConstraint& rhs) const;
  bool operator!=(const LQPUniqueConstraint& rhs) const;

  ExpressionUnorderedSet expressions;
};

using LQPUniqueConstraints = std::vector<LQPUniqueConstraint>;

}  // namespace opossum

namespace std {

// TODO(Julian) Doc: why do we need this?
template <>
struct hash<opossum::LQPUniqueConstraint> {
  size_t operator()(const opossum::LQPUniqueConstraint& unique_constraint) const {
    auto hash = boost::hash_value(unique_constraint.expressions.size());
    for (const auto& expression : unique_constraint.expressions) {
      boost::hash_combine(hash, expression->hash());
    }
    return hash;
  }
};

}  // namespace std
