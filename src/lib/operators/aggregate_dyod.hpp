#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "expression/window_function_expression.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/aggregate_dyod/accumulator_column.hpp"
#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "types.hpp"

namespace hyrise {

class AbstractAggregator;

/**
 * Parallel, radix-partitioned hash aggregation operator (GROUP BY).
 *
 * A drop-in alternative to AggregateHash: it satisfies the same AbstractAggregateOperator contract and runs against the
 * same shared aggregate_test.cpp typed test suite. The design is a port of a Rust proof-of-concept's SWWC-radix
 * pipeline into Hyrise. Execution is three scheduler rounds, each launching num_cpus JobTasks that loop on a shared
 * atomic fetch-and-add cursor to claim work one item at a time. Two hard barriers separate the rounds
 * (schedule_and_wait_for_tasks); those barriers also supply the happens-before edges the phases rely on:
 *
 *   1. ESTIMATE  Workers claim input chunks and feed per-worker HyperLogLog sketches from the packed group-by key,
 *      merged register-wise to choose the partition count P (choose_partition_count).
 *   2. SCATTER   Workers claim input chunks (one chunk == one morsel) and buffer raw (key, values...) rows into their
 *      own per-worker ScatterStore across the P partitions, via SWWC staging lines flushed with non-temporal stores.
 *      No aggregation happens here. Each worker issues one store fence before its task returns; the phase barrier
 *      alone does not make the non-temporal stores visible.
 *   3. MERGE     Workers claim partitions; for each, they stream every worker's scattered rows for that partition
 *      through a dense open-addressing MergeMap (resolve key -> dense slot, fold value into slot), tiled at
 *      MERGE_TILE_ROWS so the row->slot scratch stays L1-resident, then flush finalized groups into the worker's own
 *      thread-local OutputColumns. The output table is the concatenation of all workers' chunks.
 *
 * The scatter+merge pipeline is monomorphized once per query over the key schema chosen by resolve_key_schema(), so
 * hashing and equality are fixed, branch-free code. Queries with no GROUP BY columns take a separate reduction path
 * (no partitioning) that always emits exactly one row.
 *
 * Invariants: the chosen P is a power of two in [max(worker_count, 1), MAX_PARTITION_COUNT]. Supported aggregates are
 * SUM/MIN/MAX/AVG/COUNT/ANY over the numeric and string paths the key/accumulator schemas cover; CountDistinct and
 * StandardDeviationSample are out of scope and rejected during _prepare. description() is inherited from
 * AbstractAggregateOperator (it already emits the "GroupBy {...} FUNC(col)" format); only name() is overridden.
 *
 * Ownership/lifetime/threading: the operator instance is single-threaded until _on_execute fans out the JobTasks.
 * Within a phase each worker touches only its own ScatterStore / MergeMap / OutputColumns; cross-worker reads happen
 * only after a barrier. All scratch is freshly allocated per execution -- cross-query pooling is deferred.
 *
 * @see KeySchema, resolve_key_schema, ScatterStore, MergeMap, OutputColumns.
 * @see AggregateSchema, AbstractAccumulatorColumn.
 */
class AggregateDYOD : public AbstractAggregateOperator {
 public:
  /**
   * Constructs the operator; validation of the requested aggregates is deferred to execute time (see _prepare).
   *
   * @param input_operator  The child operator that produces the input table; shared ownership is retained (borrowed
   *   by reference here, stored by the base AbstractOperator).
   * @param aggregates  The aggregate expressions to compute (e.g. SUM(col), COUNT(*)); borrowed and copied into base
   *   operator state. Out-of-scope or invalid functions are not rejected here but during _prepare.
   * @param groupby_column_ids  Column IDs of the GROUP BY key columns, in output order; borrowed and copied into base
   *   operator state. An empty vector selects the no-GROUP-BY reduction path (_aggregate_without_group_by).
   */
  AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                const std::vector<ColumnID>& groupby_column_ids);

  /**
   * Returns the operator's name for plan printing and logging.
   *
   * @return Reference to the operator's static name string; valid for the process lifetime (not a per-call temporary).
   */
  const std::string& name() const override;

  /**
   * Pipeline phases reported through OperatorPerformanceData, in execution order.
   *
   * Estimate, Scatter, and Merge correspond to the three scheduler rounds; OutputWriting covers assembling the result
   * table from the per-worker output chunks.
   */
  enum class OperatorSteps : uint8_t { Estimate, Scatter, Merge, OutputWriting };

 protected:
  /**
   * Executes the aggregation and produces the result table; the operator's main entry point.
   *
   * Runs _prepare to build the AggregateSchema and validate aggregates, then dispatches either to the no-GROUP-BY
   * reduction path (_aggregate_without_group_by) or, via resolve_key_schema(), to the monomorphized three-phase
   * pipeline (_aggregate). Records the per-step timings named by OperatorSteps in OperatorPerformanceData.
   *
   * @return The result table: group-by columns first, then one column per aggregate. Never nullptr. With a GROUP BY it
   *   has one row per distinct key (zero rows for empty input); with no GROUP BY exactly one row (COUNT = 0 and other
   *   aggregates NULL for empty input).
   * @pre The base AbstractAggregateOperator has validated inputs and driven the state machine to execution.
   * @throws std::logic_error via Hyrise Assert()/Fail() when an aggregate is unsupported: an out-of-scope function, or
   *   an invalid function/column combination whose WindowFunctionTraits result type is DataType::Null.
   * @note Override; the base operator's execute() invokes it exactly once.
   */
  std::shared_ptr<const Table> _on_execute() override;

  std::vector<std::unique_ptr<AbstractAggregator>> _build_aggregators(
      const std::shared_ptr<const Table>& input_table) const;

  /**
   * Deep-copies this operator for plan caching / re-execution, rewiring it onto already-copied input operators.
   *
   * @param copied_left_input  The deep-copied input operator to attach as this unary operator's (left) input; shared
   *   ownership.
   * @param copied_right_input  Unused; nullptr for this unary operator.
   * @param copied_ops  Memo mapping each original operator to its copy so a shared subplan is copied once; borrowed
   *   and updated in place.
   * @return A new AggregateDYOD carrying the same aggregates and group-by columns, wired to the copied inputs.
   * @note Override; the base AbstractOperator::deep_copy drives the recursion and consults @p copied_ops.
   */
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  /**
   * Binds late-bound query parameters into this operator's expressions (prepared-statement / correlation support).
   *
   * @param parameters  Map from ParameterID to the value to bind; borrowed. Only IDs referenced by this operator's
   *   aggregate/group-by expressions are consulted; unrelated entries are ignored.
   * @note Override; called by the base before execution when a plan is re-parameterized.
   */
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  /**
   * Releases execution-scoped scratch once the result table is no longer needed.
   *
   * @note Override; called by the base operator after execution. AggregateDYOD keeps its aggregation scratch local to
   *   _on_execute, so there is no cross-query state to pool or free here.
   */
  void _on_cleanup() override;

 private:
  /**
   * Builds the AggregateSchema and output column definitions and validates the requested aggregates.
   *
   * The output columns are the group-by columns first, then one result column per aggregate. Validation reuses the base
   * AbstractAggregateOperator::_validate_aggregates() for the standard checks (e.g. SUM/AVG/STDDEV on a string column,
   * whose WindowFunctionTraits result type is DataType::Null) and adds a Fail() for the out-of-scope functions
   * (CountDistinct and StandardDeviationSample).
   *
   * @param input_table  The materialized input table, used to resolve group-by and aggregate column data types;
   *   borrowed for the duration of the call.
   * @return The AggregateSchema describing each aggregate's value stream and result type, consumed by _aggregate and
   *   _aggregate_without_group_by.
   * @pre Runs at the start of _on_execute (execute time, matching AggregateHash), not in the constructor.
   * @throws std::logic_error via Hyrise Fail()/Assert() for an out-of-scope function or an invalid function/column
   *   combination; this is the exception the shared test suite expects for invalid aggregates.
   */
  AggregateSchema _prepare(const Table& input_table);

  /**
   * Runs the full group-by pipeline (estimate -> scatter -> merge -> output), monomorphized over the key schema.
   *
   * Instantiated once per key-schema variant by resolve_key_schema(); exactly one instantiation runs per query. Drives
   * the three scheduler rounds described on the class and assembles the result table from the per-worker OutputColumns.
   *
   * @tparam KeySchema  The concrete packed-key schema (one of NumericShortKeySchema, NumericArbitraryKeySchema,
   *   MixedKeySchema, StringOnlyKeySchema) fixing pack/unpack/hash/equals for the branch-free hot loops.
   * @param key_schema  The schema instance selected for this query; borrowed for the call.
   * @param aggregate_schema  The schema produced by _prepare; borrowed. Describes value streams and result types.
   * @param input_table  The materialized input; borrowed. Source of the scattered rows.
   * @return The result table with group-by columns first, then aggregate result columns; one row per distinct key
   *   (zero rows for empty input).
   * @pre _prepare has produced @p aggregate_schema and the query has at least one group-by column (the no-GROUP-BY
   *   case uses _aggregate_without_group_by instead).
   * @note Runs on the single execution thread; it internally fans out and joins the worker JobTasks.
   */
  template <typename KeySchema>
  std::shared_ptr<Table> _aggregate(const KeySchema& key_schema, const AggregateSchema& aggregate_schema,
                                    const Table& input_table);

  /**
   * Runs the no-GROUP-BY reduction: folds all input into a single accumulator set and emits exactly one row.
   *
   * Each worker folds its claimed chunks into a local accumulator set, the sets are combined once, and one output row
   * is emitted unconditionally, so an empty input still yields one row (COUNT = 0, other aggregates NULL). Bypasses
   * partitioning, HyperLogLog, and the MergeMap entirely.
   *
   * @param aggregate_schema  The schema produced by _prepare; borrowed. Describes each aggregate's value stream and
   *   result type.
   * @param input_table  The materialized input; borrowed. May be empty.
   * @return A result table with exactly one row and one column per aggregate (no group-by columns).
   * @pre _prepare has run and the query has no group-by columns.
   */
  std::shared_ptr<Table> _aggregate_without_group_by(const AggregateSchema& aggregate_schema, const Table& input_table);
};

}  // namespace hyrise
