#include "projection.hpp"

#include <algorithm>
#include <functional>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/expression_utils.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/value_expression.hpp"
#include "storage/segment_iterate.hpp"
#include "utils/assert.hpp"

namespace opossum {

Projection::Projection(const std::shared_ptr<const AbstractOperator>& in,
                       const std::vector<std::shared_ptr<AbstractExpression>>& expressions)
    : AbstractReadOnlyOperator(OperatorType::Projection, in), expressions(expressions) {}

const std::string& Projection::name() const {
  static const auto name = std::string{"Projection"};
  return name;
}

std::shared_ptr<AbstractOperator> Projection::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Projection>(copied_input_left, expressions_deep_copy(expressions));
}

void Projection::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  expressions_set_parameters(expressions, parameters);
}

void Projection::_on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) {
  expressions_set_transaction_context(expressions, transaction_context);
}

std::shared_ptr<const Table> Projection::_on_execute() {
  const auto& input_table = *input_table_left();

  /**
   * If an expression is a PQPColumnExpression then it might be possible to forward the input column, if the
   * input TableType (References or Data) matches the output column type (ReferenceSegment or not).
   */
  const auto only_projects_columns = std::all_of(expressions.begin(), expressions.end(), [&](const auto& expression) {
    return expression->type == ExpressionType::PQPColumn;
  });

  const auto output_table_type = only_projects_columns ? input_table.type() : TableType::Data;
  const auto forward_columns = input_table.type() == output_table_type;

  const auto uncorrelated_subquery_results =
      ExpressionEvaluator::populate_uncorrelated_subquery_results_cache(expressions);

  auto column_is_nullable = std::vector<bool>(expressions.size(), false);

  /**
   * Perform the projection
   */
  auto output_chunk_segments = std::vector<Segments>(input_table.chunk_count());

  const auto chunk_count_input_table = input_table.chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count_input_table; ++chunk_id) {
    const auto input_chunk = input_table.get_chunk(chunk_id);
    Assert(input_chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    auto output_segments = Segments{expressions.size()};

    ExpressionEvaluator evaluator(input_table_left(), chunk_id, uncorrelated_subquery_results);

    for (auto column_id = ColumnID{0}; column_id < expressions.size(); ++column_id) {
      const auto& expression = expressions[column_id];

      // Forward input column if possible
      if (expression->type == ExpressionType::PQPColumn && forward_columns) {
        const auto pqp_column_expression = std::static_pointer_cast<PQPColumnExpression>(expression);
        output_segments[column_id] = input_chunk->get_segment(pqp_column_expression->column_id);
        column_is_nullable[column_id] =
            column_is_nullable[column_id] || input_table.column_is_nullable(pqp_column_expression->column_id);
      } else if (expression->type == ExpressionType::PQPColumn && !forward_columns) {
        // The current column will be returned without any logical modifications. As other columns do get modified (and
        // returned as a ValueSegment), all segments (including this one) need to become ValueSegments. This segment is
        // not yet a ValueSegment (otherwise forward_columns would be true); thus we need to materialize it.
        const auto pqp_column_expression = std::static_pointer_cast<PQPColumnExpression>(expression);
        const auto segment = input_chunk->get_segment(pqp_column_expression->column_id);

        resolve_data_type(expression->data_type(), [&](const auto data_type) {
          using ColumnDataType = typename decltype(data_type)::type;
          bool has_null = false;
          auto values = pmr_concurrent_vector<ColumnDataType>(segment->size());
          auto null_values = pmr_concurrent_vector<bool>(segment->size());

          auto chunk_offset = ChunkOffset{0};
          segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
            if (position.is_null()) {
              has_null = true;
              null_values[chunk_offset] = true;
            } else {
              values[chunk_offset] = position.value();
            }
            ++chunk_offset;
          });

          auto value_segment = std::shared_ptr<ValueSegment<ColumnDataType>>{};
          if (has_null) {
            value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(null_values));
          } else {
            value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values));
          }

          output_segments[column_id] = std::move(value_segment);
          column_is_nullable[column_id] = has_null;
        });
      } else {
        auto output_segment = evaluator.evaluate_expression_to_segment(*expression);
        column_is_nullable[column_id] = column_is_nullable[column_id] || output_segment->is_nullable();
        output_segments[column_id] = std::move(output_segment);
      }
    }

    output_chunk_segments[chunk_id] = std::move(output_segments);
  }

  /**
   * Determine the TableColumnDefinitions and build the output table
   */
  TableColumnDefinitions column_definitions;
  for (auto column_id = ColumnID{0}; column_id < expressions.size(); ++column_id) {
    column_definitions.emplace_back(expressions[column_id]->as_column_name(), expressions[column_id]->data_type(),
                                    column_is_nullable[column_id]);
  }

  auto output_chunks = std::vector<std::shared_ptr<Chunk>>{chunk_count_input_table};

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count_input_table; ++chunk_id) {
    const auto input_chunk = input_table.get_chunk(chunk_id);
    Assert(input_chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    // The output chunk contains all rows that are in the stored chunk, including invalid rows. We forward this
    // information so that following operators (currently, the Validate operator) can use it for optimizations.
    output_chunks[chunk_id] =
        std::make_shared<Chunk>(std::move(output_chunk_segments[chunk_id]), input_chunk->mvcc_data());
    output_chunks[chunk_id]->increase_invalid_row_count(input_chunk->invalid_row_count());
  }

  return std::make_shared<Table>(column_definitions, output_table_type, std::move(output_chunks),
                                 input_table.uses_mvcc());
}

// returns the singleton dummy table used for literal projections
std::shared_ptr<Table> Projection::dummy_table() {
  static auto shared_dummy = std::make_shared<DummyTable>();
  return shared_dummy;
}

}  // namespace opossum
