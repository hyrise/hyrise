#pragma once

#include "../base_test.hpp"
#include "gtest/gtest.h"
#include "storage/table_key_constraint.hpp"

namespace opossum {

/**
 * Verifies whether a given table key constraint is represented in a given set of unique constraints.
 */
static bool find_unique_constraint_by_key_constraint(const TableKeyConstraint& table_key_constraint,
                                                     std::shared_ptr<LQPUniqueConstraints> unique_constraints) {
  return std::find_if(unique_constraints->cbegin(), unique_constraints->cend(),
                      [&](const LQPUniqueConstraint& unique_constraint) {
                        // Basic comparison: Column count
                        if (table_key_constraint.columns().size() != unique_constraint.expressions.size()) return false;

                        // In-depth comparison: Column IDs
                        for (const auto& column_id : table_key_constraint.columns()) {
                          // Find column expression for column id
                          const auto& find_column_expression = std::find_if(
                              unique_constraint.expressions.cbegin(), unique_constraint.expressions.cend(),
                              [&column_id](const auto& expression) {
                                const auto& column_expression =
                                    std::dynamic_pointer_cast<LQPColumnExpression>(expression);
                                return (column_expression && column_expression->original_column_id == column_id);
                              });

                          // Check whether a column expression has been found
                          if (find_column_expression == unique_constraint.expressions.cend()) {
                            // unique constraint does not match
                            return false;
                          }
                        }

                        // unique_constraint represents table_key_constraint since none of the above checks have failed
                        return true;
                      }) != unique_constraints->cend();
}

}  // namespace opossum
