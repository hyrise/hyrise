#include <algorithm>
#include <array>
#include <cstdint>
#include <type_traits>
#include <utility>

#include "all_type_variant.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/segment_tree.hpp"
#include "window_function_combinator.hpp"
#include "window_function_evaluator.hpp"

namespace hyrise {

using ComputationStrategy = WindowFunctionEvaluator::ComputationStrategy;

WindowFunctionEvaluator::WindowFunctionEvaluator(
    const std::shared_ptr<const AbstractOperator>& input_operator, std::vector<ColumnID> init_partition_by_column_ids,
    std::vector<ColumnID> init_order_by_column_ids, ColumnID init_function_argument_column_id,
    std::shared_ptr<WindowFunctionExpression> init_window_funtion_expression)
    : AbstractReadOnlyOperator(OperatorType::WindowFunction, input_operator),
      _partition_by_column_ids(std::move(init_partition_by_column_ids)),
      _order_by_column_ids(std::move(init_order_by_column_ids)),
      _function_argument_column_id(std::move(init_function_argument_column_id)),
      _window_function_expression(std::move(init_window_funtion_expression)) {
  Assert(
      _function_argument_column_id != INVALID_COLUMN_ID || is_rank_like(_window_function_expression->window_function),
      "Could not extract window function argument, although it was not rank-like");
}

const std::string& WindowFunctionEvaluator::name() const {
  static const auto name = std::string{"WindowFunctionEvaluator"};
  return name;
}

const FrameDescription& WindowFunctionEvaluator::frame_description() const {
  const auto window = std::dynamic_pointer_cast<WindowExpression>(_window_function_expression->window());
  return *window->frame_description;
}

void WindowFunctionEvaluator::_on_set_parameters(
    [[maybe_unused]] const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  Fail("What should this do?");
}

std::shared_ptr<AbstractOperator> WindowFunctionEvaluator::_on_deep_copy(
    [[maybe_unused]] const std::shared_ptr<AbstractOperator>& copied_left_input,
    [[maybe_unused]] const std::shared_ptr<AbstractOperator>& copied_right_input,
    [[maybe_unused]] std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  Fail("What should this do?");
}

template <typename InputColumnType, WindowFunction window_function>
std::shared_ptr<const Table> WindowFunctionEvaluator::_templated_on_execute() {
  const auto input_table = left_input_table();
  const auto chunk_count = input_table->chunk_count();
  auto partitioned_data = partition_and_sort();

  using OutputColumnType = typename WindowFunctionTraits<InputColumnType, window_function>::ReturnType;

  std::vector<std::pair<pmr_vector<OutputColumnType>, pmr_vector<bool>>> segment_data_for_output_column(chunk_count);

  for (auto chunk_id = ChunkID(0); chunk_id < chunk_count; ++chunk_id) {
    const auto output_length = input_table->get_chunk(chunk_id)->size();
    segment_data_for_output_column[chunk_id].first.resize(output_length);
    segment_data_for_output_column[chunk_id].second.resize(output_length);
  }

  const auto emit_computed_value = [&](RowID row_id, std::optional<OutputColumnType> computed_value) {
    if (computed_value) {
      segment_data_for_output_column[row_id.chunk_id].first[row_id.chunk_offset] = std::move(*computed_value);
    } else {
      segment_data_for_output_column[row_id.chunk_id].second[row_id.chunk_offset] = true;
    }
  };

  switch (choose_computation_strategy<InputColumnType, window_function>()) {
    case ComputationStrategy::OnePass:
      compute_window_function_one_pass<InputColumnType, window_function>(partitioned_data, emit_computed_value);
      break;
    case ComputationStrategy::SegmentTree:
      compute_window_function_segment_tree<InputColumnType, window_function>(partitioned_data, emit_computed_value);
      break;
  }

  return annotate_input_table(std::move(segment_data_for_output_column));
}

std::shared_ptr<const Table> WindowFunctionEvaluator::_on_execute() {
  const auto window_function = _window_function_expression->window_function;
  switch (window_function) {
    case WindowFunction::Rank:
      return _templated_on_execute<NullValue, WindowFunction::Rank>();
    case WindowFunction::DenseRank:
      return _templated_on_execute<NullValue, WindowFunction::DenseRank>();
    case WindowFunction::RowNumber:
      return _templated_on_execute<NullValue, WindowFunction::RowNumber>();
    case WindowFunction::PercentRank:
      Fail("PercentRank is not supported.");
    default:
      Assert(!is_rank_like(window_function), "Unhandled rank-like window function.");
  }

  auto result = std::shared_ptr<const Table>();
  resolve_data_type(_window_function_expression->argument()->data_type(), [&](auto input_data_type) {
    using InputColumnType = typename decltype(input_data_type)::type;
    if constexpr (std::is_arithmetic_v<InputColumnType>) {
      result = [&]() {
        switch (window_function) {
          case WindowFunction::Sum:
            return _templated_on_execute<InputColumnType, WindowFunction::Sum>();
          case WindowFunction::Avg:
            return _templated_on_execute<InputColumnType, WindowFunction::Avg>();
          case WindowFunction::Min:
            return _templated_on_execute<InputColumnType, WindowFunction::Min>();
          case WindowFunction::Max:
            return _templated_on_execute<InputColumnType, WindowFunction::Max>();
          default:
            Fail("Unsupported window function.");
        }
      }();
    } else {
      Fail("Unsupported input column type for window function.");
    }
  });
  return result;
}

namespace {

template <typename T>
T clamped_add(T lhs, T rhs, T max) {
  return std::min(lhs + rhs, max);
}

template <typename T>
T clamped_sub(T lhs, T rhs, T min) {
  using U = std::make_signed_t<T>;
  return static_cast<T>(std::max(static_cast<U>(static_cast<U>(lhs) - static_cast<U>(rhs)), static_cast<U>(min)));
}

WindowFunctionEvaluator::HashPartitionedData partition_and_sort_chunk(const Chunk& chunk, ChunkID chunk_id,
                                                                      std::span<const ColumnID> partition_column_ids,
                                                                      std::span<const ColumnID> order_column_ids,
                                                                      const ColumnID function_argument_column_id) {
  auto result = WindowFunctionEvaluator::HashPartitionedData{};

  auto partition_segments = std::vector<std::shared_ptr<AbstractSegment>>();
  partition_segments.reserve(partition_column_ids.size());
  for (const auto column_id : partition_column_ids)
    partition_segments.push_back(chunk.get_segment(column_id));

  auto order_segments = std::vector<std::shared_ptr<AbstractSegment>>();
  order_segments.reserve(order_column_ids.size());
  for (const auto column_id : order_column_ids)
    order_segments.push_back(chunk.get_segment(column_id));

  for (auto chunk_offset = ChunkOffset(0), row_count = chunk.size(); chunk_offset < row_count; ++chunk_offset) {
    auto partition_values = std::vector<AllTypeVariant>();
    for (const auto& segment : partition_segments)
      partition_values.push_back((*segment)[chunk_offset]);
    auto order_values = std::vector<AllTypeVariant>();
    for (const auto& segment : order_segments)
      order_values.push_back((*segment)[chunk_offset]);

    auto function_argument = function_argument_column_id != INVALID_COLUMN_ID
                                 ? (*chunk.get_segment(function_argument_column_id))[chunk_offset]
                                 : NULL_VALUE;

    auto row_info = WindowFunctionEvaluator::RelevantRowInformation{
        .partition_values = std::move(partition_values),
        .order_values = std::move(order_values),
        .function_argument = std::move(function_argument),
        .row_id = RowID(chunk_id, chunk_offset),
    };
    const auto hash_partition = boost::hash_range(row_info.partition_values.begin(), row_info.partition_values.end()) &
                                WindowFunctionEvaluator::hash_partition_mask;
    result[hash_partition].push_back(std::move(row_info));
  }

  for (auto& partition : result) {
    std::ranges::sort(partition, WindowFunctionEvaluator::RelevantRowInformation::compare_for_hash_partitioning);
  }

  return result;
}

struct ChunkRange {
  ChunkID start;  // inclusive
  ChunkID end;    // exclusive

  ChunkID size() const {
    return ChunkID(end - start);
  }

  bool is_single_chunk() const {
    return size() == 1;
  }
};

WindowFunctionEvaluator::HashPartitionedData parallel_merge_sort(const Table& input_table, ChunkRange chunk_range,
                                                                 std::span<const ColumnID> partition_column_ids,
                                                                 std::span<const ColumnID> order_column_ids,
                                                                 ColumnID function_argument_column_id) {
  if (chunk_range.is_single_chunk()) {
    const auto chunk_id = chunk_range.start;
    const auto chunk = input_table.get_chunk(chunk_id);
    return partition_and_sort_chunk(*chunk, chunk_id, partition_column_ids, order_column_ids,
                                    function_argument_column_id);
  }

  const auto middle = ChunkID(chunk_range.start + chunk_range.size() / 2);
  auto left_result = WindowFunctionEvaluator::HashPartitionedData{};
  auto right_result = WindowFunctionEvaluator::HashPartitionedData{};
  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
  tasks.emplace_back(std::make_shared<JobTask>([&]() {
    left_result = parallel_merge_sort(input_table, ChunkRange{.start = chunk_range.start, .end = middle},
                                      partition_column_ids, order_column_ids, function_argument_column_id);
  }));
  tasks.emplace_back(std::make_shared<JobTask>([&]() {
    right_result = parallel_merge_sort(input_table, ChunkRange{.start = middle, .end = chunk_range.end},
                                       partition_column_ids, order_column_ids, function_argument_column_id);
  }));

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

  auto result = WindowFunctionEvaluator::HashPartitionedData{};

  for (auto hash_value = 0u; hash_value < WindowFunctionEvaluator::hash_partition_partition_count; ++hash_value) {
    const auto& left = left_result[hash_value];
    const auto& right = right_result[hash_value];
    auto& merged = result[hash_value];
    merged.resize(left.size() + right.size());
    std::ranges::merge(left, right, merged.begin(),
                       WindowFunctionEvaluator::RelevantRowInformation::compare_for_hash_partitioning);
  }

  return result;
}

}  // namespace

template <typename T>
void WindowFunctionEvaluator::spawn_and_wait_per_hash(const PerHash<T>& data, auto&& per_hash_function) {
  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
  for (auto hash_value = 0u; hash_value < hash_partition_partition_count; ++hash_value) {
    tasks.emplace_back(
        std::make_shared<JobTask>([hash_value, &data, &per_hash_function]() { per_hash_function(data[hash_value]); }));
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
};

std::partial_ordering WindowFunctionEvaluator::RelevantRowInformation::compare_with_null_equal(
    const AllTypeVariant& lhs, const AllTypeVariant& rhs) {
  if (variant_is_null(lhs) && variant_is_null(rhs))
    return std::partial_ordering::equivalent;
  if (variant_is_null(lhs))
    return std::partial_ordering::less;
  if (variant_is_null(rhs))
    return std::partial_ordering::greater;
  if (lhs < rhs)
    return std::partial_ordering::less;
  if (lhs == rhs)
    return std::partial_ordering::equivalent;
  return std::partial_ordering::greater;
}

std::partial_ordering WindowFunctionEvaluator::RelevantRowInformation::compare_with_null_equal(
    std::span<const AllTypeVariant> lhs, std::span<const AllTypeVariant> rhs) {
  return std::lexicographical_compare_three_way(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(),
                                                [](const auto& lhs_element, const auto& rhs_element) {
                                                  return compare_with_null_equal(lhs_element, rhs_element);
                                                });
}

bool WindowFunctionEvaluator::RelevantRowInformation::compare_for_hash_partitioning(const RelevantRowInformation& lhs,
                                                                                    const RelevantRowInformation& rhs) {
  const auto comp_result = compare_with_null_equal(lhs.partition_values, rhs.partition_values);
  if (std::is_neq(comp_result))
    return std::is_lt(comp_result);
  return std::is_lt(compare_with_null_equal(lhs.order_values, rhs.order_values));
}

template <typename InputColumnType, WindowFunction window_function>
ComputationStrategy WindowFunctionEvaluator::choose_computation_strategy() const {
  const auto& frame = frame_description();
  const auto is_prefix_frame = frame.type == FrameType::Range && frame.start.unbounded &&
                               frame.start.type == FrameBoundType::Preceding && !frame.end.unbounded &&
                               frame.end.type == FrameBoundType::CurrentRow;
  Assert(is_prefix_frame || !RankLike<window_function>, "Invalid frame for rank-like window function.");

  if (is_prefix_frame && SupportsOnePass<InputColumnType, window_function>)
    return ComputationStrategy::OnePass;

  if (SupportsSegmentTree<InputColumnType, window_function>)
    return ComputationStrategy::SegmentTree;

  Fail("Could not determine appropriate computation strategy for window function " +
       window_function_to_string.left.at(window_function) + ".");
}

bool WindowFunctionEvaluator::is_output_nullable() const {
  return !is_rank_like(_window_function_expression->window_function) &&
         left_input_table()->column_is_nullable(_function_argument_column_id);
}

WindowFunctionEvaluator::HashPartitionedData WindowFunctionEvaluator::partition_and_sort() const {
  const auto input_table = left_input_table();
  return parallel_merge_sort(*input_table, ChunkRange{.start = ChunkID(0), .end = input_table->chunk_count()},
                             _partition_by_column_ids, _order_by_column_ids, _function_argument_column_id);
}

void WindowFunctionEvaluator::for_each_partition(std::span<const RelevantRowInformation> hash_partition,
                                                 auto&& emit_partition_bounds) {
  auto partition_start = static_cast<size_t>(0);

  while (partition_start < hash_partition.size()) {
    const auto partition_end = std::distance(
        hash_partition.begin(),
        std::find_if(hash_partition.begin() + static_cast<ssize_t>(partition_start) + 1, hash_partition.end(),
                     [&](const auto& next_element) {
                       return std::is_neq(RelevantRowInformation::compare_with_null_equal(
                           hash_partition[partition_start].partition_values, next_element.partition_values));
                     }));
    emit_partition_bounds(partition_start, partition_end);
    partition_start = partition_end;
  }
}

template <typename InputColumnType, WindowFunction window_function>
void WindowFunctionEvaluator::compute_window_function_one_pass(const HashPartitionedData& partitioned_data,
                                                               auto&& emit_computed_value) const {
  if constexpr (SupportsOnePass<InputColumnType, window_function>) {
    spawn_and_wait_per_hash(partitioned_data, [&emit_computed_value](const auto& hash_partition) {
      using Traits = WindowFunctionCombinator<InputColumnType, window_function>;
      using State = typename Traits::OnePassState;
      auto state = State{};

      const RelevantRowInformation* previous_row = nullptr;

      for (const auto& row : hash_partition) {
        if (previous_row) {
          if (std::is_neq(RelevantRowInformation::compare_with_null_equal(previous_row->partition_values,
                                                                          row.partition_values)))
            state = State{};
          else
            state.update(*previous_row, row);
        }
        emit_computed_value(row.row_id, state.current_value());
        previous_row = &row;
      }
    });
  } else {
    Fail("Unsupported OnePass window function.");
  }
}

template <typename InputColumnType, WindowFunction window_function>
void WindowFunctionEvaluator::compute_window_function_segment_tree(const HashPartitionedData& partitioned_data,
                                                                   auto&& emit_computed_value) const {
  const auto& frame = frame_description();
  using OutputColumnType = typename WindowFunctionTraits<InputColumnType, window_function>::ReturnType;

  if constexpr (SupportsSegmentTree<OutputColumnType, window_function>) {
    Assert(frame.type == FrameType::Rows, "Sum only works with FrameBounds of type Rows");

    spawn_and_wait_per_hash(partitioned_data, [&emit_computed_value, &frame](const auto& hash_partition) {
      for_each_partition(hash_partition, [&](uint64_t partition_start, uint64_t partition_end) {
        using Traits = WindowFunctionCombinator<OutputColumnType, window_function>;
        using TreeNode = typename Traits::TreeNode;

        std::vector<TreeNode> leaf_values(partition_end - partition_start);
        std::transform(hash_partition.begin() + partition_start, hash_partition.begin() + partition_end,
                       leaf_values.begin(), [](const auto& row) -> TreeNode {
                         if (variant_is_null(row.function_argument))
                           return TreeNode(std::nullopt);
                         return TreeNode(static_cast<OutputColumnType>(get<InputColumnType>(row.function_argument)));
                       });
        SegmentTree<TreeNode, typename Traits::Combine> segment_tree(leaf_values, Traits::neutral_element);

        for (auto tuple_index = partition_start; tuple_index < partition_end; ++tuple_index) {
          const auto window_start = frame.start.unbounded
                                        ? partition_start
                                        : clamped_sub<uint64_t>(tuple_index, frame.start.offset, partition_start);
          const auto window_end = frame.end.unbounded
                                      ? partition_end
                                      : clamped_add<uint64_t>(tuple_index, frame.end.offset + 1, partition_end);
          using Transformer = typename Traits::QueryTransformer;
          emit_computed_value(hash_partition[tuple_index].row_id,
                              Transformer{}(segment_tree.range_query({window_start, window_end})));
        }
      });
    });
  } else {
    Fail("Unsupported SegmentTree window function.");
  }
}

template <typename T>
std::shared_ptr<const Table> WindowFunctionEvaluator::annotate_input_table(
    std::vector<std::pair<pmr_vector<T>, pmr_vector<bool>>> segment_data_for_output_column) const {
  const auto input_table = left_input_table();

  const auto chunk_count = input_table->chunk_count();
  const auto column_count = input_table->column_count();

  const auto new_column_name = _window_function_expression->as_column_name();
  const auto new_column_type = _window_function_expression->data_type();
  const auto new_column_definition = TableColumnDefinition(new_column_name, new_column_type, is_output_nullable());

  // Create value segments for our output column.
  std::vector<std::shared_ptr<AbstractSegment>> value_segments_for_new_column;
  value_segments_for_new_column.reserve(chunk_count);
  for (auto chunk_id = ChunkID(0); chunk_id < chunk_count; ++chunk_id) {
    if (is_output_nullable()) {
      value_segments_for_new_column.emplace_back(
          std::make_shared<ValueSegment<T>>(std::move(segment_data_for_output_column[chunk_id].first),
                                            std::move(segment_data_for_output_column[chunk_id].second)));
    } else {
      value_segments_for_new_column.emplace_back(
          std::make_shared<ValueSegment<T>>(std::move(segment_data_for_output_column[chunk_id].first)));
    }
  }

  auto outputted_segments_for_new_column = std::vector<std::shared_ptr<AbstractSegment>>{};
  if (input_table->type() == TableType::Data) {
    // If our input table is of TableType::Data, use the value segments created above for our output table.
    outputted_segments_for_new_column = std::move(value_segments_for_new_column);
  } else {
    // If our input table is of TableType::Reference, create an extra table with the value segments and use reference
    // segments to this table in our output.
    auto chunks_for_referenced_table = std::vector<std::shared_ptr<Chunk>>{};
    chunks_for_referenced_table.reserve(chunk_count);
    for (auto chunk_id = ChunkID(0); chunk_id < chunk_count; ++chunk_id) {
      chunks_for_referenced_table.emplace_back(
          std::make_shared<Chunk>(Segments({std::move(value_segments_for_new_column[chunk_id])})));
    }
    auto referenced_table = std::make_shared<Table>(TableColumnDefinitions({new_column_definition}), TableType::Data,
                                                    std::move(chunks_for_referenced_table));

    for (auto chunk_id = ChunkID(0); chunk_id < chunk_count; ++chunk_id) {
      const auto pos_list = std::make_shared<EntireChunkPosList>(chunk_id, input_table->get_chunk(chunk_id)->size());
      outputted_segments_for_new_column.emplace_back(
          std::make_shared<ReferenceSegment>(referenced_table, ColumnID{0}, pos_list));
    }
  }

  // Create output table reusing the segments from input and the newly created segments for the output column
  auto output_column_definitions = input_table->column_definitions();
  output_column_definitions.push_back(new_column_definition);
  std::vector<std::shared_ptr<Chunk>> output_chunks;
  output_chunks.reserve(chunk_count);
  for (auto chunk_id = ChunkID(0); chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = input_table->get_chunk(chunk_id);
    Segments output_segments;
    for (auto column_id = ColumnID(0); column_id < column_count; ++column_id) {
      output_segments.emplace_back(chunk->get_segment(column_id));
    }
    output_segments.push_back(std::move(outputted_segments_for_new_column[chunk_id]));

    output_chunks.emplace_back(std::make_shared<Chunk>(std::move(output_segments)));
  }

  return std::make_shared<Table>(output_column_definitions, input_table->type(), std::move(output_chunks));
}

}  // namespace hyrise
