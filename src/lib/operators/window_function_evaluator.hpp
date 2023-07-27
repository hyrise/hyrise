#pragma once

#include <memory>
#include <string>
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
#include "window_function_evaluator/util.hpp"
#include "window_function_evaluator/window_function_evaluator_traits.hpp"

namespace hyrise {

namespace window_function_evaluator {

class WindowFunctionEvaluator : public AbstractReadOnlyOperator {
 public:
  WindowFunctionEvaluator(const std::shared_ptr<const AbstractOperator>& input_operator,
                          std::vector<ColumnID> init_partition_by_column_ids,
                          std::vector<ColumnID> init_order_by_column_ids, ColumnID init_function_argument_column_id,
                          std::shared_ptr<WindowFunctionExpression> init_window_funtion_expression);

  const std::string& name() const override;
  bool is_output_nullable() const;

  enum class ComputationStrategy {
    OnePass,
    SegmentTree,
  };

  template <typename InputColumnType, WindowFunction window_function>
  ComputationStrategy choose_computation_strategy() const;

  enum class OperatorSteps : uint8_t { PartitionAndSort, Compute, Annotate };

  struct PerformanceData : public OperatorPerformanceData<OperatorSteps> {
    void output_to_stream(std::ostream& stream, DescriptionMode description_mode) const override;
    ComputationStrategy computation_strategy;
  };

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  template <typename InputColumnType, WindowFunction window_function>
  std::shared_ptr<const Table> _templated_on_execute();

  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_left_input,
      const std::shared_ptr<AbstractOperator>& copied_right_input,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;

 private:
  HashPartitionedData partition_and_sort() const;

  template <typename InputColumnType, WindowFunction window_function>
    requires SupportsOnePass<InputColumnType, window_function>
  void compute_window_function_one_pass(const HashPartitionedData& partitioned_data, auto&& emit_computed_value) const;

  template <typename InputColumnType, WindowFunction window_function>
    requires SupportsSegmentTree<InputColumnType, window_function>
  void compute_window_function_segment_tree(const HashPartitionedData& partitioned_data,
                                            auto&& emit_computed_value) const;

  template <typename OutputColumnType>
  std::shared_ptr<const Table> annotate_input_table(
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
