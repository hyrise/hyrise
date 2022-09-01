#include "projection.hpp"

#include <algorithm>
#include <atomic>
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
#include "hyrise.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/resolve_encoded_segment_type.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"

namespace hyrise {

Projection::Projection(const std::shared_ptr<const AbstractOperator>& input_operator,
                       const std::vector<std::shared_ptr<AbstractExpression>>& init_expressions)
    : AbstractReadOnlyOperator(OperatorType::Projection, input_operator, nullptr,
                               std::make_unique<OperatorPerformanceData<OperatorSteps>>()),
      expressions(init_expressions) {
  /**
   * Register as a consumer for all uncorrelated subqueries.
   * In contrast, we do not register for correlated subqueries which cannot be reused by design. They are fully owned
   * and managed by the ExpressionEvaluator.
   */
  for (const auto& expression : expressions) {
    auto pqp_subquery_expressions = find_pqp_subquery_expressions(expression);
    for (const auto& subquery_expression : pqp_subquery_expressions) {
      if (subquery_expression->is_correlated()) {
        continue;
      }

      /**
       * Uncorrelated subqueries will be resolved when Projection::_on_execute is called. Therefore, we
       * 1. register as a consumer and
       * 2. store pointers to call ExpressionEvaluator::populate_uncorrelated_subquery_results_cache later on.
       */
      subquery_expression->pqp->register_consumer();
      _uncorrelated_subquery_expressions.push_back(subquery_expression);
    }
  }
}

const std::string& Projection::name() const {
  static const auto name = std::string{"Projection"};
  return name;
}

std::shared_ptr<AbstractOperator> Projection::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& copied_right_input,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  // Passing copied_ops is essential to allow for global subplan deduplication, including subqueries.
  return std::make_shared<Projection>(copied_left_input, expressions_deep_copy(expressions, copied_ops));
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

  // Uncorrelated subqueries need to be evaluated exactly once, not once per chunk.
  const auto uncorrelated_subquery_results =
      ExpressionEvaluator::populate_uncorrelated_subquery_results_cache(_uncorrelated_subquery_expressions);
  // Deregister, because we obtained the results and no longer need the subquery plans.
  for (const auto& pqp_subquery_expression : _uncorrelated_subquery_expressions) {
    pqp_subquery_expression->pqp->deregister_consumer();
  }

  auto& step_performance_data = dynamic_cast<OperatorPerformanceData<OperatorSteps>&>(*performance_data);
  if (!uncorrelated_subquery_results->empty()) {
    step_performance_data.set_step_runtime(OperatorSteps::UncorrelatedSubqueries, timer.lap());
  }

  auto forwarding_cost = std::chrono::nanoseconds{};
  auto expression_evaluator_cost = std::chrono::nanoseconds{};

  const auto chunk_count = input_table.chunk_count();

  // Perform the actual projection on a per-chunk level. `output_segments_by_chunk` will contain both forwarded and
  // newly generated columns. In the upcoming loop, we do not yet deal with the projection_result_table indirection
  // described above.
  auto output_segments_by_chunk = std::vector<Segments>(chunk_count);

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(chunk_count);

  const auto expression_count = expressions.size();
  const auto forwarded_pqp_columns = _determine_forwarded_columns(output_table_type);

  // NULLability information is either forwarded or collected during the execution of the ExpressionEvaluator. The
  // vector stores atomic bool values. This allows parallel write operation per thread.
  auto column_is_nullable = std::vector<std::atomic_bool>(expressions.size());

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto input_chunk = input_table.get_chunk(chunk_id);
    Assert(input_chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    auto output_segments = Segments{expression_count};
    auto all_segments_forwarded = true;

    for (auto column_id = ColumnID{0}; column_id < expression_count; ++column_id) {
      // In this loop, we perform all projections that only forward an input column sequential.
      const auto& expression = expressions[column_id];
      if (!forwarded_pqp_columns.contains(expression)) {
        all_segments_forwarded = false;
        continue;
      }

      DebugAssert(std::dynamic_pointer_cast<PQPColumnExpression>(expression),
                  "Non-PQP column expressions should not reach this point.");

      // Forward input segment if possible
      const auto& pqp_column_expression = static_cast<const PQPColumnExpression&>(*expression);
      output_segments[column_id] = input_chunk->get_segment(pqp_column_expression.column_id);
      column_is_nullable[column_id] = input_table.column_is_nullable(pqp_column_expression.column_id);
    }
    forwarding_cost += timer.lap();

    // `output_segments_by_chunk` now contains all forwarded segments.
    output_segments_by_chunk[chunk_id] = std::move(output_segments);

    // All columns are forwarded. We do not need to evaluate newly generated columns.
    if (all_segments_forwarded) {
      continue;
    }

    // Defines the job that performs the evaluation if the columns are newly generated.
    auto perform_projection_evaluation = [this, chunk_id, &uncorrelated_subquery_results, expression_count,
                                          &output_segments_by_chunk, &column_is_nullable, &forwarded_pqp_columns]() {
      auto evaluator = ExpressionEvaluator{left_input_table(), chunk_id, uncorrelated_subquery_results};

      for (auto column_id = ColumnID{0}; column_id < expression_count; ++column_id) {
        const auto& expression = expressions[column_id];

        if (!forwarded_pqp_columns.contains(expression)) {
          // Newly generated column - the expression needs to be evaluated
          auto output_segment = evaluator.evaluate_expression_to_segment(*expression);
          column_is_nullable[column_id] = column_is_nullable[column_id] || output_segment->is_nullable();
          // Storing the result in output_segments_by_chunk means that the vector for the separate chunks may contain
          // both ReferenceSegments and ValueSegments. We deal with this later.
          output_segments_by_chunk[chunk_id][column_id] = std::move(output_segment);
        }
      }
    };
    // Evaluate the expression immediately if it contains less than `JOB_SPAWN_THRESHOLD` rows, otherwise wrap
    // it into a task. The upper bound of the chunk size, which defines if it will be executed in parallel or not,
    // still needs to be re-evaluated over time to find the value which gives the best performance.
    constexpr auto JOB_SPAWN_THRESHOLD = ChunkOffset{500};
    if (input_chunk->size() >= JOB_SPAWN_THRESHOLD) {
      auto job_task = std::make_shared<JobTask>(perform_projection_evaluation);
      jobs.push_back(job_task);
    } else {
      perform_projection_evaluation();
      expression_evaluator_cost += timer.lap();
    }
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  expression_evaluator_cost += timer.lap();

  step_performance_data.set_step_runtime(OperatorSteps::ForwardUnmodifiedColumns, forwarding_cost);
  step_performance_data.set_step_runtime(OperatorSteps::EvaluateNewColumns, expression_evaluator_cost);

  // Determine the TableColumnDefinitions. We can only do this now because column_is_nullable has been filled in the
  // loop above. If necessary, projection_result_column_definitions holds those newly generated columns that the
  // ReferenceSegments point to.
  TableColumnDefinitions output_column_definitions;
  TableColumnDefinitions projection_result_column_definitions;
  for (auto column_id = ColumnID{0}; column_id < expression_count; ++column_id) {
    const auto definition = TableColumnDefinition{expressions[column_id]->as_column_name(),
                                                  expressions[column_id]->data_type(), column_is_nullable[column_id]};
    output_column_definitions.emplace_back(definition);

    if (!forwarded_pqp_columns.contains(expressions[column_id]) && output_table_type == TableType::References) {
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

  // Create a mapping from output columns to input columns for future use. This is necessary as the order may have been
  // changed. The mapping only contains column IDs that are forwarded without modfications.
  auto output_column_to_input_column = std::unordered_map<ColumnID, ColumnID>{};
  for (auto expression_id = ColumnID{0}; expression_id < expression_count; ++expression_id) {
    const auto& expression = expressions[expression_id];
    if (const auto pqp_column_expression = std::dynamic_pointer_cast<PQPColumnExpression>(expression)) {
      if (forwarded_pqp_columns.contains(expression)) {
        const auto& original_id = pqp_column_expression->column_id;
        output_column_to_input_column[expression_id] = original_id;
      }
    }
  }

  // Create the actual chunks, and, if needed, fill the projection_result_table. Also set MVCC and
  // individually_sorted_by information as needed.
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto input_chunk = input_table.get_chunk(chunk_id);
    Assert(input_chunk, "Physically deleted chunk should not reach this point, see get_chunk / #1686.");

    auto projection_result_segments = Segments{};
    const auto entire_chunk_pos_list = std::make_shared<EntireChunkPosList>(chunk_id, input_chunk->size());
    for (auto column_id = ColumnID{0}; column_id < expression_count; ++column_id) {
      // Turn newly generated ValueSegments into ReferenceSegments, if needed
      if (!forwarded_pqp_columns.contains(expressions[column_id]) && output_table_type == TableType::References) {
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

      // We need to iterate both sorted information and the output/input mapping as multiple output columns might
      // originate from the same sorted input column.
      for (const auto& [output_column_id, input_column_id] : output_column_to_input_column) {
        const auto iter = std::find_if(
            sorted_by.begin(), sorted_by.end(),
            [input_column_id = input_column_id](const auto sort) { return input_column_id == sort.column; });
        if (iter != sorted_by.end()) {
          transformed.emplace_back(SortColumnDefinition{output_column_id, iter->sort_mode});
        }
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

/**
 *  Method to determine PQPColumns to forward. As explained above, we forward columns that are simply projected and
 *  need not be evaluated. But there are cases when forwarding is not beneficial. When a forwardable column is
 *  also evaluated in an expression, the expression evaluator materializes this column and caches it. In case of having
 *  a reference segment as input, forwarding the materialized and cached segment has a similar performance in the
 *  projection operator, but is faster in the following operator. The reason is that the following operator does not
 *  need to process the forwarded reference segment via its position list indirection but can directly access the value
 *  segment sequentially.
 */
ExpressionUnorderedSet Projection::_determine_forwarded_columns(const TableType table_type) const {
  // First gather all forwardable PQP column expressions.
  auto forwarded_pqp_columns = ExpressionUnorderedSet{};
  const auto expression_count = expressions.size();
  for (auto column_id = ColumnID{0}; column_id < expression_count; ++column_id) {
    const auto& expression = expressions[column_id];
    if (expression->type == ExpressionType::PQPColumn) {
      forwarded_pqp_columns.emplace(expression);
    }
  }

  // Iterate the expressions and check if a forwarded column is part of an expression. In this case, remove it from
  // the list of forwarded columns. When the input is a data table (and thus the output table is as well) the
  // forwarded column does not need to be accessed via its position list later. And since the following operator might
  // have optimizations for accessing an encoded segment, we always forward for data tables.
  if (table_type == TableType::References) {
    for (auto column_id = ColumnID{0}; column_id < expression_count; ++column_id) {
      const auto& expression = expressions[column_id];

      if (expression->type == ExpressionType::PQPColumn) {
        continue;
      }

      visit_expression(expression, [&](const auto& sub_expression) {
        if (sub_expression->type == ExpressionType::PQPColumn) {
          forwarded_pqp_columns.erase(sub_expression);
        }
        return ExpressionVisitation::VisitArguments;
      });
    }
  }

  return forwarded_pqp_columns;
}

}  // namespace hyrise
