#include "limit.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/expression_utils.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"

namespace opossum {

Limit::Limit(const std::shared_ptr<const AbstractOperator>& in,
             const std::shared_ptr<AbstractExpression>& row_count_expression)
    : AbstractReadOnlyOperator(OperatorType::Limit, in), _row_count_expression(row_count_expression) {}

const std::string Limit::name() const { return "Limit"; }

std::shared_ptr<AbstractExpression> Limit::row_count_expression() const { return _row_count_expression; }

std::shared_ptr<AbstractOperator> Limit::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Limit>(copied_input_left, _row_count_expression->deep_copy());
}

std::shared_ptr<const Table> Limit::_on_execute() {
  const auto input_table = input_table_left();

  /**
   * Evaluate the _row_count_expression to determine the actual number of rows to "Limit" the output to
   */
  auto num_rows = size_t{};

  resolve_data_type(_row_count_expression->data_type(), [&](const auto data_type_t) {
    using LimitDataType = typename decltype(data_type_t)::type;

    if constexpr (std::is_integral_v<LimitDataType>) {
      const auto num_rows_expression_result =
          ExpressionEvaluator{}.evaluate_expression_to_result<LimitDataType>(*_row_count_expression);
      Assert(num_rows_expression_result->size() == 1, "Expected exactly one row for Limit");
      Assert(!num_rows_expression_result->is_null(0), "Expected non-null for Limit");

      const auto signed_num_rows = num_rows_expression_result->value(0);
      Assert(signed_num_rows >= 0, "Can't Limit to a negative number of Rows");

      num_rows = static_cast<size_t>(signed_num_rows);
    } else {
      Fail("Non-integral types not allowed in Limit");
    }
  });

  /**
   * Perform the actual limitting
   */
  auto output_chunks = std::vector<std::shared_ptr<Chunk>>{};

  ChunkID chunk_id{0};
  const auto chunk_count = input_table->chunk_count();
  for (size_t i = 0; i < num_rows && chunk_id < chunk_count; chunk_id++) {
    const auto input_chunk = input_table->get_chunk(chunk_id);
    Assert(input_chunk, "Did not expect deleted chunk here.");  // see #1686

    Segments output_segments;

    size_t output_chunk_row_count = std::min<size_t>(input_chunk->size(), num_rows - i);

    for (ColumnID column_id{0}; column_id < input_table->column_count(); column_id++) {
      const auto input_base_segment = input_chunk->get_segment(column_id);
      auto output_pos_list = std::make_shared<PosList>(output_chunk_row_count);
      std::shared_ptr<const Table> referenced_table;
      ColumnID output_column_id = column_id;

      if (auto input_ref_segment = std::dynamic_pointer_cast<const ReferenceSegment>(input_base_segment)) {
        output_column_id = input_ref_segment->referenced_column_id();
        referenced_table = input_ref_segment->referenced_table();
        // TODO(all): optimize using whole chunk whenever possible
        auto begin = input_ref_segment->pos_list()->begin();
        std::copy(begin, begin + output_chunk_row_count, output_pos_list->begin());
      } else {
        referenced_table = input_table;
        for (ChunkOffset chunk_offset = 0; chunk_offset < output_chunk_row_count; chunk_offset++) {
          (*output_pos_list)[chunk_offset] = RowID{chunk_id, chunk_offset};
        }
      }

      output_segments.push_back(
          std::make_shared<ReferenceSegment>(referenced_table, output_column_id, output_pos_list));
    }

    i += output_chunk_row_count;
    output_chunks.emplace_back(std::make_shared<Chunk>(std::move(output_segments)));
  }

  return std::make_shared<Table>(input_table->column_definitions(), TableType::References, std::move(output_chunks));
}

void Limit::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  expression_set_parameters(_row_count_expression, parameters);
}

void Limit::_on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) {
  expression_set_transaction_context(_row_count_expression, transaction_context);
}

}  // namespace opossum
