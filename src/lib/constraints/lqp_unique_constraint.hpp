#pragma once

#include <functional>
#include <vector>
#include "expression/abstract_expression.hpp"
#include "storage/constraints/table_constraint_definition.hpp"
#include "types.hpp"

namespace opossum {

// Defines a unique constraint on a set of abstract expressions.
struct LQPUniqueConstraint final {
  explicit LQPUniqueConstraint(ExpressionUnorderedSet init_column_expressions)
      : column_expressions(std::move(init_column_expressions)) {}

  bool operator==(const LQPUniqueConstraint& rhs) const {
    if (column_expressions.size() != rhs.column_expressions.size()) return false;
    return std::all_of(column_expressions.cbegin(), column_expressions.cend(), [&rhs](const auto column_expression) {
      return rhs.column_expressions.contains(column_expression);
    });
  }
  bool operator!=(const LQPUniqueConstraint& rhs) const { return !(rhs == *this); }

  ExpressionUnorderedSet column_expressions;
};

using LQPUniqueConstraints = std::unordered_set<LQPUniqueConstraint>;

}  // namespace opossum

namespace std {

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
