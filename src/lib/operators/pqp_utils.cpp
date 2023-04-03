#include "pqp_utils.hpp"

#include "storage/segment_iterate.hpp"

namespace hyrise {

std::vector<std::shared_ptr<AbstractOperator>> pqp_find_operators_by_type(const std::shared_ptr<AbstractOperator>& pqp,
                                                                          const OperatorType type) {
  auto operators = std::vector<std::shared_ptr<AbstractOperator>>{};
  visit_pqp(pqp, [&](const auto& op) {
    if (op->type() == type) {
      operators.emplace_back(op);
    }
    return PQPVisitation::VisitInputs;
  });

  return operators;
}

AllTypeVariant resolve_uncorrelated_subquery(const std::shared_ptr<const AbstractOperator>& subquery_operator) {
  auto subquery_result = NULL_VALUE;
  const auto& subquery_result_table = subquery_operator->get_output();
  const auto row_count = subquery_result_table->row_count();
  Assert(subquery_result_table->column_count() == 1 && row_count <= 1,
         "Uncorrelated subqueries may return at most one single value.");

  if (row_count == 1) {
    const auto chunk = subquery_result_table->get_chunk(ChunkID{0});
    Assert(chunk, "Subquery results cannot be physically deleted.");
    resolve_data_type(subquery_result_table->column_data_type(ColumnID{0}), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      segment_iterate<ColumnDataType>(*chunk->get_segment(ColumnID{0}), [&](const auto& position) {
        if (!position.is_null()) {
          subquery_result = position.value();
        }
      });
    });
  }

  return subquery_result;
}

}  // namespace hyrise
