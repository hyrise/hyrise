#pragma once

#include "../base_test.hpp"
#include "constraints/table_key_constraint.hpp"
#include "gtest/gtest.h"

namespace opossum {

/**
 * Checks whether a given set of TableKeyConstraints is represented in a given set of LQPUniqueConstraints.
 */
static void check_unique_constraint_mapping(const TableKeyConstraints& table_key_constraints,
                                            const std::shared_ptr<LQPUniqueConstraints> unique_constraints) {
  for (const auto& table_key_constraint : table_key_constraints) {
    const auto matching_unique_constraint = std::find_if(
        unique_constraints->cbegin(), unique_constraints->cend(), [&](const LQPUniqueConstraint& unique_constraint) {
          // Basic comparison
          if (table_key_constraint.columns().size() != unique_constraint.expressions.size()) return false;

          // In-depth comparison, verifying column ids
          for (const auto& column_id : table_key_constraint.columns()) {
            // Try to find a column_expression that represents column_id
            const auto matching_column_expr = std::find_if(
                unique_constraint.expressions.cbegin(), unique_constraint.expressions.cend(),
                [&](const std::shared_ptr<AbstractExpression>& expression) {
                  const auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
                  if (column_expression && column_expression->original_column_id == column_id) {
                    return true;
                  } else {
                    return false;
                  }
                });

            // Check whether a column expression has been found
            if (matching_column_expr == unique_constraint.expressions.cend()) {
              // unique_constraint does not match table_key_constraint
              return false;
            }
          }

          // unique_constraint represents table_key_constraint since none of the above checks have failed
          return true;
        });

    // Check whether a matching unique_constraint has been found
    EXPECT_FALSE(matching_unique_constraint == unique_constraints->cend());
  }
}

}  // namespace opossum
