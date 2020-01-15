#pragma once

#include <functional>
#include <vector>
#include "expression/abstract_expression.hpp"
#include "storage/constraints/table_constraint_definition.hpp"
#include "types.hpp"

namespace opossum {

// Defines a unique constraint on a set of abstract expressions. Can optionally be a PRIMARY KEY.

struct ExpressionsConstraintDefinition final {
  ExpressionsConstraintDefinition(ExpressionUnorderedSet init_column_expressions, const IsPrimaryKey is_primary_key)
      : column_expressions(std::move(init_column_expressions)), is_primary_key(is_primary_key) {}

  bool operator==(const ExpressionsConstraintDefinition& rhs) const {
    return column_expressions == rhs.column_expressions && is_primary_key == rhs.is_primary_key;
  }
  bool operator!=(const ExpressionsConstraintDefinition& rhs) const { return !(rhs == *this); }

  size_t hash() const {
    auto hash = boost::hash_value(is_primary_key);
    for (const auto& expression : column_expressions) {
      boost::hash_combine(hash, expression->hash());
    }
    return hash;
  }

  ExpressionUnorderedSet column_expressions;
  IsPrimaryKey is_primary_key;
};

// Wrapper-structs for enabling hash based containers containing ExpressionsConstraintDefinition
struct ExpressionsConstraintDefinitionHash final {
  size_t operator()(const ExpressionsConstraintDefinition& expressions_constraint_definition) const {
    return expressions_constraint_definition.hash();
  }
};
struct ExpressionsConstraintDefinitionEqual final {
  size_t operator()(const ExpressionsConstraintDefinition& constraint_a,
                    const ExpressionsConstraintDefinition& constraint_b) const {
    return constraint_a == constraint_b;
  }
};

using ExpressionsConstraintDefinitions =
    std::unordered_set<ExpressionsConstraintDefinition, ExpressionsConstraintDefinitionHash,
                       ExpressionsConstraintDefinitionEqual>; // TODO analog ändern

}  // namespace opossum
