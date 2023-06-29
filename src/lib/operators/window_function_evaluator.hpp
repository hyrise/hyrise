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
#include "storage/table.hpp"

namespace hyrise {

class WindowFunctionEvaluator : public AbstractReadOnlyOperator {
 public:
  WindowFunctionEvaluator(const std::shared_ptr<const AbstractOperator>& input_operator,
                          std::vector<ColumnID> init_partition_by_column_ids,
                          std::vector<ColumnID> init_order_by_column_ids,
                          std::shared_ptr<WindowFunctionExpression> init_window_funtion_expression);

  const std::string& name() const override;

  static constexpr auto initial_rank =
      static_cast<typename WindowFunctionTraits<void, WindowFunction::Rank>::ReturnType>(1);

  static constexpr uint8_t hash_partition_bits = 8;
  static constexpr size_t hash_partition_mask = (1u << hash_partition_bits) - 1;
  static constexpr uint32_t hash_partition_partition_count = 1u << hash_partition_bits;

  template <typename T>
  using PerHash = std::array<T, hash_partition_partition_count>;

  template <typename T>
  static void spawn_and_wait_per_hash(const PerHash<T>& data, auto&& per_hash_function);

  struct RelevantRowInformation {
    std::vector<AllTypeVariant> partition_values;
    std::vector<AllTypeVariant> order_values;
    AllTypeVariant function_argument;
    RowID row_id;

    static bool compare_for_hash_partitioning(const RelevantRowInformation& lhs, const RelevantRowInformation& rhs);
  };

  using HashPartitionedData = PerHash<std::vector<RelevantRowInformation>>;

  enum class ComputationStrategy {
    OnePass,
    SegmentTree,
  };

  template <WindowFunction window_function>
  ComputationStrategy choose_computation_strategy() const;

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
  void compute_window_function(const HashPartitionedData& partitioned_data, auto&& emit_computed_value) const;

  static void for_each_partition(std::span<const RelevantRowInformation> hash_partition, auto&& emit_partition_bounds);

  template <typename T>
  std::shared_ptr<const Table> annotate_input_table(
      std::vector<std::pair<pmr_vector<T>, pmr_vector<bool>>> segment_data_for_output_column) const;

  const FrameDescription& frame_description() const;

  std::vector<ColumnID> _partition_by_column_ids;
  std::vector<ColumnID> _order_by_column_ids;
  std::shared_ptr<WindowFunctionExpression> _window_function_expression;
};

}  // namespace hyrise
