#pragma once

#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/evaluation/expression_result.hpp"
#include "expression/value_expression.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

inline AllTypeVariant _get_value(const Table& table, const RowID row_id, const ColumnID& column_id) {
  const auto& segment = *table.get_chunk(row_id.chunk_id)->segments()[column_id];
  return segment[row_id.chunk_offset];
}

inline bool _fulfills_join_predicates(const Table& left_table, const Table& right_table, const RowID left_row_id,
                                      const RowID right_row_id, const std::vector<JoinPredicate>& join_predicates) {
  if (join_predicates.empty()) {
    return true;
  }

  const auto& evaluator = std::make_shared<ExpressionEvaluator>();

  for (const auto& join_predicate : join_predicates) {
    const auto left_value = _get_value(left_table, left_row_id, join_predicate.column_id_pair.first);
    const auto right_value = _get_value(right_table, right_row_id, join_predicate.column_id_pair.second);
    const auto left_value_expression = std::make_shared<ValueExpression>(left_value);
    const auto right_value_expression = std::make_shared<ValueExpression>(right_value);
    const auto predicate_expression = std::make_shared<BinaryPredicateExpression>(
        join_predicate.predicate_condition, left_value_expression, right_value_expression);
    const auto result = evaluator->evaluate_expression_to_result<ExpressionEvaluator::Bool>(*predicate_expression);

    if (!result->value(0)) {
      return false;
    }
  }

  return true;
}

}  // namespace opossum
