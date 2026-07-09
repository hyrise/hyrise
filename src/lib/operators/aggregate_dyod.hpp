#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/aggregate_dyod/accumulator_column.hpp"
#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "expression/window_function_expression.hpp"
#include "types.hpp"

namespace hyrise {

// Parallel, radix-partitioned hash aggregation operator (GROUP BY), porting a Rust proof-of-concept's SWWC-radix design
// into Hyrise. Drop-in alternative to AggregateHash: it satisfies the same AbstractAggregateOperator contract and runs
// against the same shared aggregate_test.cpp typed test suite. description() is inherited from AbstractAggregateOperator
// (it already emits the required "GroupBy {...} FUNC(col)" format); only name() is overridden here.
//
// Execution is three scheduler rounds, each num_cpus JobTasks looping on a shared atomic fetch-and-add cursor to claim
// work one item at a time. There are two hard barriers (the schedule_and_wait_for_tasks between rounds), which also
// provide the happens-before edges the phases rely on:
//
//   1. ESTIMATE  workers claim input chunks and feed per-worker HyperLogLog sketches from the packed group-by key;
//                merged register-wise to choose the partition count P (choose_partition_count).
//   2. SCATTER   workers claim input chunks (one chunk == one morsel) and buffer raw (key, values...) rows into their
//                own per-worker ScatterStore across the P partitions, via SWWC staging + non-temporal stores. No
//                aggregation happens here. Each worker issues a store fence before its task returns (NT visibility).
//   3. MERGE     workers claim partitions; for each, stream every worker's scattered rows for that partition through a
//                dense MergeMap (resolve key -> slot, fold value into slot), then flush finalized groups into the
//                worker's own local OutputColumns. The output table is the concatenation of all workers' chunks.
//
// The scatter+merge pipeline is monomorphized once per query over the key schema chosen by resolve_key_schema(), so
// hashing and equality are fixed, branch-free code. Queries with no GROUP BY columns take a separate reduction path
// (no partitioning) that always emits exactly one row.
//
// v1 scope: aggregates SUM/MIN/MAX/AVG/COUNT over the numeric and string paths the key/accumulator schemas cover;
// CountDistinct, StandardDeviationSample, and Any are out of scope (rejected at construction). Cross-query scratch
// pooling is deferred (fresh allocation per execution).
class AggregateDYOD : public AbstractAggregateOperator {
 public:
  AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                const std::vector<ColumnID>& groupby_column_ids);

  const std::string& name() const override;

  // Steps reported via OperatorPerformanceData.
  enum class OperatorSteps : uint8_t { Estimate, Scatter, Merge, OutputWriting };

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  void _on_cleanup() override;

 private:
  // Build the AggregateSchema and the output column definitions (group-by columns first, then aggregate result
  // columns), and validate the requested aggregates. Runs at the start of _on_execute (execute time, matching
  // AggregateHash), NOT in the constructor. Validation reuses the base AbstractAggregateOperator::_validate_aggregates()
  // for the standard checks (e.g. SUM/AVG/STDDEV on a string column, whose WindowFunctionTraits result type is
  // DataType::Null) and adds a Fail() for the v1-out-of-scope functions (CountDistinct/StandardDeviationSample/Any);
  // Hyrise's Fail()/Assert throw std::logic_error, which is what the test suite expects for invalid combinations.
  AggregateSchema _prepare(const Table& input_table);

  // The full group-by pipeline (estimate -> scatter -> merge -> stitch), monomorphized over the concrete key schema.
  // Instantiated once per key-schema variant via resolve_key_schema(); one instantiation runs per query.
  template <typename KeySchema>
  std::shared_ptr<Table> _aggregate(const KeySchema& key_schema, const AggregateSchema& aggregate_schema,
                                    const Table& input_table);

  // The no-GROUP-BY reduction path: each worker folds all its claimed chunks into one local accumulator set, the sets
  // are combined once, and exactly one output row is emitted -- unconditionally, so an empty input still yields one row
  // (COUNT = 0, others NULL). Bypasses partitioning, HyperLogLog, and the merge map entirely.
  std::shared_ptr<Table> _aggregate_without_group_by(const AggregateSchema& aggregate_schema, const Table& input_table);
};

}  // namespace hyrise
