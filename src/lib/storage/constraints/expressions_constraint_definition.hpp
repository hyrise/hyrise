#pragma once

#include <functional>
#include <vector>
#include "expression/abstract_expression.hpp"
#include "storage/constraints/table_constraint_definition.hpp"
#include "types.hpp"

namespace opossum {

// Defines a unique constraint on a set of abstract expressions. Can optionally be a PRIMARY KEY.

struct ExpressionsConstraintDefinition final {
  ExpressionsConstraintDefinition(ExpressionUnorderedSet init_column_expressions, const IsPrimaryKey is_primary_key, ExpressionUnorderedSet init_column_expressions_functionally_dependent = {})
      : column_expressions(std::move(init_column_expressions)),
      is_primary_key(is_primary_key),
      column_expressions_functionally_dependent(std::move(init_column_expressions_functionally_dependent)) {}

  bool operator==(const ExpressionsConstraintDefinition& rhs) const {
    return column_expressions == rhs.column_expressions && is_primary_key == rhs.is_primary_key &&
    column_expressions_functionally_dependent == rhs.column_expressions_functionally_dependent;
  }
  bool operator!=(const ExpressionsConstraintDefinition& rhs) const { return !(rhs == *this); }

  ExpressionUnorderedSet column_expressions;
  IsPrimaryKey is_primary_key;

  // Models a variant of functional dependencies:
  // In case the unique constraint applies, functionally dependent column expressions are also considered unique.
  ExpressionUnorderedSet column_expressions_functionally_dependent;
};

using ExpressionsConstraintDefinitions = std::unordered_set<ExpressionsConstraintDefinition>;

}  // namespace opossum

namespace std {

template <>
struct hash<opossum::ExpressionsConstraintDefinition> {
  size_t operator()(const opossum::ExpressionsConstraintDefinition& constraint) const {
    auto hash = boost::hash_value(constraint.is_primary_key);
    for (const auto& expression : constraint.column_expressions) {
      boost::hash_combine(hash, expression->hash());
    }
    for (const auto& expression : constraint.column_expressions_functionally_dependent) {
      boost::hash_combine(hash, expression->hash());
    }
    return hash;
  }
};

} // namespace std
