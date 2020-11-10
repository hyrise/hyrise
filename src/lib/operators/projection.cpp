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
#include "storage/resolve_encoded_segment_type.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"

namespace opossum {

Projection::Projection(const std::shared_ptr<const AbstractOperator>& input_operator,
                       const std::vector<std::shared_ptr<AbstractExpression>>& init_expressions)
    : AbstractReadOnlyOperator(OperatorType::Projection, input_operator, nullptr,
                               std::make_unique<OperatorPerformanceData<OperatorSteps>>()),
      expressions(init_expressions) {}

const std::string& Projection::name() const {
  static const auto name = std::string{"Projection"};
  return name;
}

std::shared_ptr<AbstractOperator> Projection::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input) const {
  return std::make_shared<Projection>(copied_left_input, expressions_deep_copy(expressions));
}

void Projection::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  expressions_set_parameters(expressions, parameters);
}

void Projection::_on_set_transaction_context(const std::weak_ptr<TransactionContext>& transaction_context) {
  expressions_set_transaction_context(expressions, transaction_context);
}

std::shared_ptr<const Table> Projection::_on_execute() {
  Timer timer;

  const auto& input_table = *left_input_table();

  // Determine the type of the output table: If no input columns are forwarded, i.e., all output columns are newly
  // generated, the output type is always TableType::Data and all segments are ValueSegments. Otherwise, the output type
  // depends on the input table's type. If the forwarded columns come from a data table, so are the newly generated
  // columns. However, we cannot return a table that mixes data and reference segments, so if the forwarded columns are
  // reference columns, the newly generated columns contain reference segments, too. These point to an internal dummy
  // table, the projection_result_table. The life time of this table is managed by the shared_ptr in
  // ReferenceSegment::_referenced_table. The ReferenceSegments created for this use an EntireChunkPosList, so
  // segment_iterate on the ReferenceSegment decays to the underlying ValueSegment virtually without overhead.

  //           +-----+     +------+  Case 1
  //           |a |b |     |a |a+1|   * Input TableType::Data
  //           |DS|DS| --> |DS|VS |   * A column (a) is forwarded
  //           +-----+     +------+   Result: No type change needed, output TableType::Data
  //
  //           +-----+        +---+  Case 2
  //           |a |b |        |a+1|   * Input TableType::References
  //           |RS|RS| -->    |VS |   * No column is forwarded
  //           +-----+        +---+   Result: Output TableType::Data
  //
  // +------+  +-----+     +------+  Case 3
  // |orig_a|  |a |b |     |a |a+1|   * Input TableType::References
  // |VS    |  |RS|RS| --> |RS|RS |   * A column (a) is forwarded
  // +------+  +-----+     +------+   Result: Type change needed, output TableType::References, RS a is forwarded, a+1
  //    ^        |          |   |             is a new RS pointing to a new dummy table.
  //    +--------+----------+   v
  //                          +---+  (VS: ValueSegment, DS: DictionarySegment, RS: ReferenceSegment)
  //                          |a+1|
  //                          |VS |
  //                          +---+

  const auto forwards_any_columns = std::any_of(expressions.begin(), expressions.end(), [&](const auto& expression) {
    return expression->type == ExpressionType::PQPColumn;
  });
  const auto output_table_type = forwards_any_columns ? input_table.type() : TableType::Data;

  // NULLability information is either forwarded or collected during the execution of the ExpressionEvaluator
  auto column_is_nullable = std::vector<bool>(expressions.size(), false);

  // Uncorrelated subqueries need to be evaluated exactly once, not once per chunk.
  const auto uncorrelated_subquery_results =
      ExpressionEvaluator::populate_uncorrelated_subquery_results_cache(expressions);

  auto& step_performance_data = dynamic_cast<OperatorPerformanceData<OperatorSteps>&>(*performance_data);
  if (!uncorrelated_subquery_results->empty()) {
    step_performance_data.set_step_runtime(OperatorSteps::UncorrelatedSubqueries, timer.lap());
  }

  // Perform the actual projection on a per-chunk level. `output_segments_by_chunk` will contain both forwarded and
  // newly generated columns. In the upcoming loop, we do not yet deal with the projection_result_table indirection
  // described above.
  auto output_segments_by_chunk = std::vector<Segments>(input_table.chunk_count());

  auto forwarding_cost = std::chrono::nanoseconds{};
  auto expression_evaluator_cost = std::chrono::nanoseconds{};

  const auto chunk_count = input_table.chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto input_chunk = input_table.get_chunk(chunk_id);
    Assert(input_chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    auto output_segments = Segments{expressions.size()};

    // The ExpressionEvaluator is created once per chunk so that evaluated sub-expressions can be reused across columns.
    ExpressionEvaluator evaluator(left_input_table(), chunk_id, uncorrelated_subquery_results);

    for (auto column_id = ColumnID{0}; column_id < expressions.size(); ++column_id) {
      const auto& expression = expressions[column_id];

      if (expression->type == ExpressionType::PQPColumn) {
        // Forward input column if possible
        const auto& pqp_column_expression = static_cast<const PQPColumnExpression&>(*expression);
        output_segments[column_id] = input_chunk->get_segment(pqp_column_expression.column_id);
        column_is_nullable[column_id] =
            column_is_nullable[column_id] || input_table.column_is_nullable(pqp_column_expression.column_id);
        forwarding_cost += timer.lap();
      } else {
        // Newly generated column - the expression needs to be evaluated
        auto output_segment = evaluator.evaluate_expression_to_segment(*expression);
        column_is_nullable[column_id] = column_is_nullable[column_id] || output_segment->is_nullable();

        // Storing the result in output_segments means that the vector may contain both ReferenceSegments and
        // ValueSegments. We deal with this later.
        output_segments[column_id] = std::move(output_segment);
        expression_evaluator_cost += timer.lap();
      }
    }

    output_segments_by_chunk[chunk_id] = std::move(output_segments);
  }

  step_performance_data.set_step_runtime(OperatorSteps::ForwardUnmodifiedColumns, forwarding_cost);
  step_performance_data.set_step_runtime(OperatorSteps::EvaluateNewColumns, expression_evaluator_cost);

  // Determine the TableColumnDefinitions. We can only do this now because column_is_nullable has been filled in the
  // loop above. If necessary, projection_result_column_definitions holds those newly generated columns that the
  // ReferenceSegments point to.
  TableColumnDefinitions output_column_definitions;
  TableColumnDefinitions projection_result_column_definitions;
  for (auto column_id = ColumnID{0}; column_id < expressions.size(); ++column_id) {
    const auto definition = TableColumnDefinition{expressions[column_id]->as_column_name(),
                                                  expressions[column_id]->data_type(), column_is_nullable[column_id]};
    output_column_definitions.emplace_back(definition);

    if (expressions[column_id]->type != ExpressionType::PQPColumn && output_table_type == TableType::References) {
      projection_result_column_definitions.emplace_back(definition);
    }
  }

  // Create the projection_result_table if needed
  auto projection_result_table = std::shared_ptr<Table>{};
  if (!projection_result_column_definitions.empty()) {
    projection_result_table = std::make_shared<Table>(projection_result_column_definitions, TableType::Data,
                                                      std::nullopt, input_table.uses_mvcc());
  }

  auto output_chunks = std::vector<std::shared_ptr<Chunk>>{chunk_count};
  auto projection_result_chunks = std::vector<std::shared_ptr<Chunk>>{chunk_count};

  // Create a mapping from input columns to output columns for future use. This is necessary as the order may have been
  // changed. The mapping only contains input column IDs that are forwarded to the output without modfications.
  auto input_column_to_output_column = std::unordered_map<ColumnID, ColumnID>{};
  for (auto expression_id = ColumnID{0}; expression_id < expressions.size(); ++expression_id) {
    const auto& expression = expressions[expression_id];
    if (const auto pqp_column_expression = std::dynamic_pointer_cast<PQPColumnExpression>(expression)) {
      const auto& original_id = pqp_column_expression->column_id;
      input_column_to_output_column[original_id] = expression_id;
    }
  }

  // Create the actual chunks, and, if needed, fill the projection_result_table. Also set MVCC and
  // individually_sorted_by information as needed.
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto input_chunk = input_table.get_chunk(chunk_id);
    Assert(input_chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    auto projection_result_segments = Segments{};
    const auto entire_chunk_pos_list = std::make_shared<EntireChunkPosList>(chunk_id, input_chunk->size());
    for (auto column_id = ColumnID{0}; column_id < expressions.size(); ++column_id) {
      // Turn newly generated ValueSegments into ReferenceSegments, if needed
      if (expressions[column_id]->type != ExpressionType::PQPColumn && output_table_type == TableType::References) {
        projection_result_segments.emplace_back(output_segments_by_chunk[chunk_id][column_id]);

        const auto projection_result_column_id =
            ColumnID{static_cast<ColumnID::base_type>(projection_result_segments.size() - 1)};

        output_segments_by_chunk[chunk_id][column_id] = std::make_shared<ReferenceSegment>(
            projection_result_table, projection_result_column_id, entire_chunk_pos_list);
      }
    }

    // The output chunk contains all rows that are in the stored chunk, including invalid rows. We forward this
    // information so that following operators (currently, the Validate operator) can use it for optimizations.
    auto chunk = std::shared_ptr<Chunk>{};
    if (output_table_type == TableType::Data) {
      chunk = std::make_shared<Chunk>(std::move(output_segments_by_chunk[chunk_id]), input_chunk->mvcc_data());
      chunk->increase_invalid_row_count(input_chunk->invalid_row_count());
      chunk->finalize();

      DebugAssert(projection_result_segments.empty(),
                  "For TableType::Data, projection_result_segments should be unused");
    } else {
      chunk = std::make_shared<Chunk>(std::move(output_segments_by_chunk[chunk_id]));
      // No need to increase_invalid_row_count here, as it is ignored for reference chunks anyway
      chunk->finalize();

      if (projection_result_table) {
        projection_result_table->append_chunk(projection_result_segments, input_chunk->mvcc_data());
        projection_result_table->last_chunk()->increase_invalid_row_count(input_chunk->invalid_row_count());
      }
    }

    // Forward sorted_by flags, mapping column ids
    const auto& sorted_by = input_chunk->individually_sorted_by();
    if (!sorted_by.empty()) {
      std::vector<SortColumnDefinition> transformed;
      transformed.reserve(sorted_by.size());
      for (const auto& [column_id, mode] : sorted_by) {
        if (!input_column_to_output_column.count(column_id)) {
          continue;  // column is not present in output expression list
        }
        const auto projected_column_id = input_column_to_output_column[column_id];
        transformed.emplace_back(SortColumnDefinition{projected_column_id, mode});
      }
      if (!transformed.empty()) {
        chunk->set_individually_sorted_by(transformed);
      }
    }

    output_chunks[chunk_id] = chunk;
  }

  step_performance_data.set_step_runtime(OperatorSteps::BuildOutput, timer.lap());

  return std::make_shared<Table>(output_column_definitions, output_table_type, std::move(output_chunks),
                                 input_table.uses_mvcc());
}

// returns the singleton dummy table used for literal projections
std::shared_ptr<Table> Projection::dummy_table() {
  static auto shared_dummy = std::make_shared<DummyTable>();
  return shared_dummy;
}

}  // namespace opossum
