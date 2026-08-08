#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "expression/window_function_expression.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "operators/aggregate_dyod/aggregate_schema.hpp"
#include "types.hpp"

namespace hyrise {

class AbstractAggregator;

/**
 * Parallel, radix-partitioned hash aggregation operator.
 *
 * Our implementation contains three distinct execution paths. One execution path for queries without GROUP BY columns,
 * one for the case where the output has a low cardinality and one for the high cardinality case. The high cardinality
 * phase is divided into three different phases:
 *
 *   1. ESTIMATE: Workers claim input chunks and feed per-worker HyperLogLog sketches from the packed group-by key,
 *      merged register-wise to choose the partition count P (choose_partition_count). The chosen P is a power of two
 *      in [max(worker_count, 1), MAX_PARTITION_COUNT]. The potential dispatch into the low cardinality phase happens
 *      after this phase.
 *   2. SCATTER: Workers claim input chunks (one chunk == one morsel) and buffer raw (key, values...) rows into their
 *      own per-worker ScatterStore across the P partitions, via SWWC staging lines flushed with non-temporal stores.
 *      No aggregation happens here. Each worker issues one store fence before its task returns; the phase barrier
 *      alone does not make the non-temporal stores visible.
 *   3. MERGE: Workers claim partitions; for each, they stream every worker's scattered rows for that partition
 *      through a dense open-addressing MergeMap (resolve key -> dense slot, fold value into slot), tiled at
 *      merge_tile_rows() so the row->slot scratch stays L1-resident, then flush finalized groups into the worker's own
 *      thread-local OutputColumns. The output table is the concatenation of all workers' chunks.
 *
 * The scatter+merge pipeline is monomorphized once per query over the key schema chosen by resolve_key_schema(), so
 * hashing and equality are fixed, branch-free code.
 */
class AggregateDYOD : public AbstractAggregateOperator {
 public:
  AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                const std::vector<ColumnID>& groupby_column_ids);

  const std::string& name() const override;

  // Pipeline phases reported through OperatorPerformanceData, in execution order.
  enum class OperatorSteps : uint8_t { Estimate, Scatter, Merge, OutputWriting };

 protected:
  /**
   * Runs _prepare to build the AggregateSchema and validate aggregates, then dispatches either to the no-GROUP-BY
   * reduction path (_aggregate_without_group_by) or, via resolve_key_schema(), to the monomorphized three-phase
   * pipeline (_aggregate).
   */
  std::shared_ptr<const Table> _on_execute() override;

  std::vector<std::unique_ptr<AbstractAggregator>> _build_aggregators(
      const std::shared_ptr<const Table>& input_table) const;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_cleanup() override;

 private:
  /**
   * Builds the AggregateSchema and validates the requested aggregates.
   */
  AggregateSchema _prepare(const Table& input_table);

  /**
   * Runs the full group-by pipeline for the high cardinality case (estimate -> scatter -> merge -> output),
   * monomorphized over the key schema. This might dispatch to _aggregate_low_cardinality for low cardinality results.
   *
   * Instantiated once per key-schema variant by resolve_key_schema(); exactly one instantiation runs per query. Drives
   * the three phases described on the class and assembles the result table from the per-worker OutputColumns.
   */
  template <typename KeySchema>
  std::shared_ptr<Table> _aggregate(const KeySchema& key_schema, const AggregateSchema& aggregate_schema,
                                    const Table& input_table);

  /**
   * When the cardinality estimate is below low_cardinality_threshold() this execution path is taken. Partitioning via
   * Scatter is skipped and each worker repeatedly takes chunks, folds them into a thread-local MergeMap and in the end
   * all MergeMaps of the workers are combined into a single MergeMap which is then used for output generation.
   */
  template <typename KeySchema>
  std::shared_ptr<Table> _aggregate_low_cardinality(const KeySchema& key_schema,
                                                    const AggregateSchema& aggregate_schema, const Table& input_table,
                                                    size_t cardinality_estimate);

  /**
   * Runs the no-GROUP-BY reduction: folds all input into a single accumulator set and emits exactly one row.
   *
   * Each worker folds its claimed chunks into a local accumulator set, the sets are combined once, and one output row
   * is emitted unconditionally. Bypasses partitioning, cardinality estimation, and the MergeMap entirely.
   */
  std::shared_ptr<Table> _aggregate_without_group_by(const AggregateSchema& aggregate_schema, const Table& input_table);
};

}  // namespace hyrise
