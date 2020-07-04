#pragma once

#include "expression/abstract_expression.hpp"
#include "types.hpp"

namespace opossum {

/**
 * Container structure to define uniqueness for subsets of LQP output expressions.
 * Uniqueness means TODO(Julian)
 */
struct LQPUniqueConstraint final {
  explicit LQPUniqueConstraint(ExpressionUnorderedSet init_column_expressions);

  bool operator==(const LQPUniqueConstraint& rhs) const;
  bool operator!=(const LQPUniqueConstraint& rhs) const;

  ExpressionUnorderedSet column_expressions;
};

using LQPUniqueConstraints = std::vector<LQPUniqueConstraint>;

}  // namespace opossum

namespace std {

// TODO(Julian) Doc: why do we need this?
template <>
struct hash<opossum::LQPUniqueConstraint> {
  size_t operator()(const opossum::LQPUniqueConstraint& constraint) const {
    auto hash = boost::hash_value(constraint.column_expressions.size());
    for (const auto& expression : constraint.column_expressions) {
      boost::hash_combine(hash, expression->hash());
    }
    return hash;
  }
};

}  // namespace std
