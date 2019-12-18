#pragma once

#include <vector>
#include "types.hpp"
#include "storage/constraints/table_constraint_definition.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

// Defines a unique constraint on a set of abstract expressions.
// Can optionally be a PRIMARY KEY, requiring the column(s) to be non-NULL.

struct ExpressionsConstraintDefinition final {
  ExpressionsConstraintDefinition(std::vector<std::shared_ptr<AbstractExpression>> init_column_expressions, const IsPrimaryKey is_primary_key)
      : column_expressions(std::move(init_column_expressions)), is_primary_key(is_primary_key) {
    Assert(std::unique(column_expressions.begin(), column_expressions.end()) == column_expressions.end(), "Duplicate column expressions unexpected.");
  }

  [[nodiscard]] bool equals(const ExpressionsConstraintDefinition& other_constraint) const {
    if(is_primary_key != other_constraint.is_primary_key) return false;
    if(column_expressions.size() != other_constraint.column_expressions.size()) return false;

    // TODO(Julian) Implement element-by-element comparison

    return true;
  }

  std::vector<std::shared_ptr<AbstractExpression>> column_expressions;
  IsPrimaryKey is_primary_key;
};

using ExpressionsConstraintDefinitions = std::vector<ExpressionsConstraintDefinition>;

}  // namespace opossum
