#pragma once

#include "../base_test.hpp"
#include "gtest/gtest.h"
#include "storage/table_key_constraint.hpp"

namespace opossum {

/**
 * Verifies whether a given table key constraint is represented in a given set of unique constraints.
 */
static bool find_unique_constraint_by_key_constraint(const TableKeyConstraint& table_key_constraint,
                                                     const std::shared_ptr<LQPUniqueConstraints>& unique_constraints) {
  const auto& column_ids = table_key_constraint.columns();

  for (const auto& unique_constraint : *unique_constraints) {
    // Basic comparison: Column count
    if (column_ids.size() != unique_constraint.expressions.size()) continue;

    // In-depth comparison: Column IDs
    auto unique_constraint_column_ids = std::unordered_set<ColumnID>();
    for (const auto& expression : unique_constraint.expressions) {
      const auto& column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
      if (column_expression) {
        unique_constraint_column_ids.emplace(column_expression->original_column_id);
      }
    }

    if (unique_constraint_column_ids == column_ids) {
      return true;
    }
  }

  // Did not find a matching unique constraint
  return false;
}

}  // namespace opossum
