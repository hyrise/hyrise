#pragma once

#include "../base_test.hpp"

namespace hyrise {

/**
 * Verifies whether a given table key constraint is represented in a given set of unique column combinations.
 */
static bool find_ucc_by_key_constraint(const TableKeyConstraint& table_key_constraint,
                                       const UniqueColumnCombinations& unique_column_combinations) {
  const auto& column_ids = table_key_constraint.columns();

  for (const auto& ucc : unique_column_combinations) {
    // Basic comparison: Column count.
    if (column_ids.size() != ucc.expressions.size()) {
      continue;
    }

    // In-depth comparison: Column IDs.
    auto ucc_column_ids = std::set<ColumnID>();
    for (const auto& expression : ucc.expressions) {
      const auto& column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
      if (column_expression) {
        ucc_column_ids.emplace(column_expression->original_column_id);
      }
    }

    if (ucc_column_ids == column_ids) {
      return true;
    }
  }

  // Did not find a matching UCC.
  return false;
}

}  // namespace hyrise
