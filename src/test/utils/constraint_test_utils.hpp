#pragma once

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../src/lib/storage/constraints/table_constraint_definition.hpp"

namespace opossum {

/**
 * Checks, whether all given TableConstraintDefinition objects are represented in ExpressionsConstraintDefinitions.
 */
static void check_table_constraint_representation(
    const TableConstraintDefinitions& table_constraints,
    const std::shared_ptr<ExpressionsConstraintDefinitions> lqp_constraints) {
  for (const auto& table_constraint : table_constraints) {
    const auto matching_lqp_constraint = std::find_if(
        lqp_constraints->cbegin(), lqp_constraints->cend(), [&](const ExpressionsConstraintDefinition& lqp_constraint) {
          // Basic comparison
          if (table_constraint.columns.size() != lqp_constraint.column_expressions.size()) return false;

          // In-depth comparison, verifying column ids
          for (const auto& column_id : table_constraint.columns) {
            // Try to find a column_expression that represents column_id
            const auto matching_column_expr = std::find_if(
                lqp_constraint.column_expressions.cbegin(), lqp_constraint.column_expressions.cend(),
                [&](const std::shared_ptr<AbstractExpression>& expression) {
                  const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
                  if (column_expression && column_expression->original_column_id == column_id) {
                    return true;
                  } else {
                    return false;
                  }
                });

            // Check whether a column expression has been found
            if (matching_column_expr == lqp_constraint.column_expressions.cend()) {
              // lqp_constraint does not match table_constraint
              return false;
            }
          }

          // lqp_constraint represents table_constraint since none of the above checks have failed
          return true;
        });

    // Check whether a matching lqp_constraint has been found
    EXPECT_FALSE(matching_lqp_constraint == lqp_constraints->cend());
  }
}

}  // namespace opossum
