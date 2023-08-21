#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "abstract_operator.hpp"
#include "abstract_read_only_operator.hpp"
#include "all_type_variant.hpp"
#include "concurrency/transaction_context.hpp"
#include "expression/window_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "operators/aggregate/window_function_traits.hpp"
#include "operators/operator_performance_data.hpp"
#include "storage/table.hpp"
#include "utils/make_bimap.hpp"
#include "window_function_evaluator/util.hpp"
#include "window_function_evaluator/window_function_evaluator_traits.hpp"

namespace hyrise {

namespace window_function_evaluator {

enum class ComputationStrategy {
  OnePass,
  SegmentTree,
};

std::ostream& operator<<(std::ostream& stream, const ComputationStrategy computation_strategy);

const auto computation_strategy_to_string = make_bimap<ComputationStrategy, std::string_view>({
    {ComputationStrategy::OnePass, "OnePass"},
    {ComputationStrategy::SegmentTree, "SegmentTree"},
});

// Evaluates window function expressions, like
//
//     name, AVG(price + tax) OVER (
//       PARTITION BY store_id
//       ORDER BY first_seen_in_shelf_data
//       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
//     )
//     FROM products
//
// or
//
//     name, RANK() OVER (
//       PARTITION BY store_id
//       ORDER BY tax DESC
//     )
//     FROM products
//
// The operator is based on the paper
//
//     "Efficient Processing of Window Functions in Analytical SQL Queries" (Leis et. al., 2015)
//
// and runs in four steps:
//
//     1. Input materialization
//
//        The output of the input_operator is materialized. We only keep the columns that are relevant for the
//        computation (see `RelevantRowInformation`). The rows are partitioned into buckets according to a hash value
//        derived from the values in the partition-by columns. Since the calculation for each tuple only cares about
//        tuples in the same partition, the buckets can then be processed in parallel. Note that multiple partitions
//        might end up in the same bucket. However, we will make sure that each partition is continuous in memory in the
//        next step.
//
//     2. Partition and order
//
//        The tuples in each bucket are sorted according to their values in the partition-by and the order-by columns.
//        While the relative ordering of the partitions is irrelevant, we need to make sure that the partitions are
//        grouped in memory, so that we can tell where all tuples from one partition are (and when a new partition
//        starts). As a secondary sorting key, the order-by columns are used. Note that this step uses a stable sorting
//        algorithm, because SQLite does so and we want to produce the same output as SQLite for testing.
//
//     3. Compute window function
//
//        The data is partitioned and ordered, now we can start computing the output values for each tuple. There are
//        two strategies for doing so:
//
//        a) One pass
//
//           In the `OnePass` strategy, the partition is scanned only once and the output for each tuple is computed on
//           the fly. This is used for window functions like RANK, where the output is only determined by the ordering
//           of the tuples. It can also be used for computing any other function with a prefix frame (from the start of
//           the partition up to the current row), but this is currently not used, as this step does not take
//           significant amounts of time anyways.
//
//        b) Segment tree
//
//           If the `OnePass` strategy cannot be used (for example, because the frame is more complicated), the
//           `SegmentTree` strategy is used instead. For each partition, we first construct a segment tree according to
//           the desired window function (see `segment_tree.hpp` for an introduction to the data structure). Afterwards,
//           we iterate over the partition again and perform a range query on the segment tree for the range that
//           matches the tuples frame. Determining the frame for each tuple is either a constant offset (ROWS mode) or
//           two binary searches in the partition (RANGE mode).
//
//        To enable computation with either of the two strategies, a number of requirements must be met in the
//        `WindowFunctionEvaluatorTraits` (see `SupportsOnePass` and `SupportsSegmentTree). Adding support for a new
//        window function boils down to implementing the required methods in `window_function_evaluator_traits.hpp`.
//
//     4. Annotate input table
//
//        Finally, an output table is constructed which contains the newly computed column. This needs to adhere to the
//        `Table` invariants about data and reference segments.
//
//  In `OperatorSteps`, there is actually one more step, `Cleanup`, which is used to measure the time to free the
//  materialized input data once the other steps are finished. This is due to the fact that we found this time to be
//  surprisingly high.
class WindowFunctionEvaluator : public AbstractReadOnlyOperator {
 public:
  WindowFunctionEvaluator(const std::shared_ptr<const AbstractOperator>& input_operator,
                          std::vector<ColumnID> init_partition_by_column_ids,
                          std::vector<ColumnID> init_order_by_column_ids, ColumnID init_function_argument_column_id,
                          std::shared_ptr<WindowFunctionExpression> init_window_funtion_expression);

  const std::string& name() const override;
  std::string description(DescriptionMode description_mode) const override;
  bool is_output_nullable() const;

  template <typename InputColumnType, WindowFunction window_function>
  ComputationStrategy choose_computation_strategy() const;

  enum class OperatorSteps : uint8_t {
    MaterializeIntoBuckets,
    PartitionAndOrder,
    Compute,
    Annotate,
    // It seems that freeing the materialized input takes up a significant amount of time, so it gets its own step.
    Cleanup,
  };

  struct PerformanceData : public OperatorPerformanceData<OperatorSteps> {
    void output_to_stream(std::ostream& stream, DescriptionMode description_mode) const override;
    ComputationStrategy computation_strategy;
  };

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  template <typename InputColumnType, WindowFunction window_function>
  std::shared_ptr<const Table> _templated_on_execute();

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override {}

  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

 private:
  [[nodiscard]] Buckets materialize_into_buckets() const;
  void partition_and_order(Buckets& buckets) const;

  template <typename InputColumnType, WindowFunction window_function>
    requires SupportsOnePass<InputColumnType, window_function>
  void compute_window_function_one_pass(const Buckets& buckets, auto&& emit_computed_value) const;

  template <typename InputColumnType, WindowFunction window_function>
    requires SupportsSegmentTree<InputColumnType, window_function>
  void compute_window_function_segment_tree(const Buckets& buckets, auto&& emit_computed_value) const;

  template <typename OutputColumnType>
  [[nodiscard]] std::shared_ptr<const Table> annotate_input_table(
      std::vector<std::pair<pmr_vector<OutputColumnType>, pmr_vector<bool>>> segment_data_for_output_column) const;

  const FrameDescription& frame_description() const;

  std::vector<ColumnID> _partition_by_column_ids;
  std::vector<ColumnID> _order_by_column_ids;
  ColumnID _function_argument_column_id;
  std::shared_ptr<WindowFunctionExpression> _window_function_expression;
};

}  // namespace window_function_evaluator

using WindowFunctionEvaluator = window_function_evaluator::WindowFunctionEvaluator;

}  // namespace hyrise
