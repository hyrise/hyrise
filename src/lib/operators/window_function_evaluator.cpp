#include <algorithm>
#include <cstdint>
#include <iterator>
#include <type_traits>
#include <utility>

#include "all_type_variant.hpp"
#include "expression/window_function_expression.hpp"
#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "scheduler/job_task.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/format_duration.hpp"
#include "utils/segment_tree.hpp"
#include "utils/timer.hpp"
#include "window_function_evaluator.hpp"

namespace hyrise::window_function_evaluator {

WindowFunctionEvaluator::WindowFunctionEvaluator(
    const std::shared_ptr<const AbstractOperator>& input_operator, std::vector<ColumnID> init_partition_by_column_ids,
    std::vector<ColumnID> init_order_by_column_ids, ColumnID init_function_argument_column_id,
    std::shared_ptr<WindowFunctionExpression> init_window_funtion_expression)
    : AbstractReadOnlyOperator(OperatorType::WindowFunction, input_operator, nullptr,
                               std::make_unique<PerformanceData>()),
      _partition_by_column_ids(std::move(init_partition_by_column_ids)),
      _order_by_column_ids(std::move(init_order_by_column_ids)),
      _function_argument_column_id(init_function_argument_column_id),
      _window_function_expression(std::move(init_window_funtion_expression)) {
  Assert(
      _function_argument_column_id != INVALID_COLUMN_ID || is_rank_like(_window_function_expression->window_function),
      "Could not extract window function argument, although it was not rank-like.");
}

const std::string& WindowFunctionEvaluator::name() const {
  static const auto name = std::string{"WindowFunctionEvaluator"};
  return name;
}

const FrameDescription& WindowFunctionEvaluator::frame_description() const {
  const auto window = std::dynamic_pointer_cast<WindowExpression>(_window_function_expression->window());
  return window->frame_description;
}

std::shared_ptr<AbstractOperator> WindowFunctionEvaluator::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    [[maybe_unused]] const std::shared_ptr<AbstractOperator>& copied_right_input,
    [[maybe_unused]] std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<WindowFunctionEvaluator>(copied_left_input, _partition_by_column_ids, _order_by_column_ids,
                                                   _function_argument_column_id, _window_function_expression);
}

template <typename InputColumnType, WindowFunction window_function>
std::shared_ptr<const Table> WindowFunctionEvaluator::_templated_on_execute() {
  if (!is_rank_like(window_function) && frame_description().type == FrameType::Range) {
    // The SQL standard only allows for at most one order-by column when using range mode.
    // If no order-by column is provided all tuples are treated as peers.
    Assert(_order_by_column_ids.size() <= 1,
           "For non-rank-like window functions, range mode frames are only allowed when there is at most one order-by "
           "expression.");
  }

  const auto input_table = left_input_table();
  const auto chunk_count = input_table->chunk_count();

  auto& window_performance_data = dynamic_cast<PerformanceData&>(*performance_data);

  auto timer = Timer{};

  auto partitioned_data = materialize_into_buckets();
  window_performance_data.set_step_runtime(OperatorSteps::MaterializeIntoBuckets, timer.lap());

  partition_and_order(partitioned_data);
  window_performance_data.set_step_runtime(OperatorSteps::PartitionAndOrder, timer.lap());

  using OutputColumnType = typename WindowFunctionEvaluatorTraits<InputColumnType, window_function>::OutputColumnType;

  auto segment_data_for_output_column =
      std::vector<std::pair<pmr_vector<OutputColumnType>, pmr_vector<bool>>>(chunk_count);

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

  const auto computation_strategy = choose_computation_strategy<InputColumnType, window_function>();
  window_performance_data.computation_strategy = computation_strategy;

  switch (computation_strategy) {
    case ComputationStrategy::OnePass:
      if constexpr (SupportsOnePass<InputColumnType, window_function>) {
        compute_window_function_one_pass<InputColumnType, window_function>(partitioned_data, emit_computed_value);
      } else {
        Fail("Chose ComputationStrategy::OnePass, although it is not supported!");
      }
      break;
    case ComputationStrategy::SegmentTree:
      if constexpr (SupportsSegmentTree<InputColumnType, window_function>) {
        compute_window_function_segment_tree<InputColumnType, window_function>(partitioned_data, emit_computed_value);
      } else {
        Fail("Chose ComputationStrategy::SegmentTree, although it is not supported!");
      }
      break;
  }
  window_performance_data.set_step_runtime(OperatorSteps::Compute, timer.lap());

  const auto annotated_table = annotate_input_table(std::move(segment_data_for_output_column));
  window_performance_data.set_step_runtime(OperatorSteps::Annotate, timer.lap());

  return annotated_table;
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
          case WindowFunction::Count:
            return _templated_on_execute<InputColumnType, WindowFunction::Count>();
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

}  // namespace

template <typename InputColumnType, WindowFunction window_function>
ComputationStrategy WindowFunctionEvaluator::choose_computation_strategy() const {
  const auto& frame = frame_description();
  const auto is_prefix_frame = frame.start.unbounded && frame.start.type == FrameBoundType::Preceding &&
                               !frame.end.unbounded && frame.end.type == FrameBoundType::CurrentRow;
  Assert(is_prefix_frame || !is_rank_like(window_function), "Invalid frame for rank-like window function.");

  if (is_prefix_frame && SupportsOnePass<InputColumnType, window_function> &&
      (is_rank_like(window_function) || frame.type == FrameType::Rows)) {
    return ComputationStrategy::OnePass;
  }

  if (SupportsSegmentTree<InputColumnType, window_function>) {
    return ComputationStrategy::SegmentTree;
  }

  Fail("Could not determine appropriate computation strategy for window function " +
       window_function_to_string.left.at(window_function) + ".");
}

bool WindowFunctionEvaluator::is_output_nullable() const {
  return !is_rank_like(_window_function_expression->window_function) &&
         left_input_table()->column_is_nullable(_function_argument_column_id);
}

namespace {

HashPartitionedData collect_chunk_into_buckets(ChunkID chunk_id, const Chunk& chunk,
                                               std::span<const ColumnID> partition_column_ids,
                                               std::span<const ColumnID> order_column_ids,
                                               ColumnID function_argument_column_id) {
  const auto chunk_size = chunk.size();

  const auto collect_values_of_columns = [&chunk, chunk_size](std::span<const ColumnID> column_ids) {
    const auto column_count = column_ids.size();
    auto values = std::vector(chunk_size, std::vector(column_count, NULL_VALUE));
    for (auto column_id_index = 0u; column_id_index < column_count; ++column_id_index) {
      const auto segment = chunk.get_segment(column_ids[column_id_index]);
      segment_iterate(*segment, [&](const auto& position) {
        if (!position.is_null()) {
          values[position.chunk_offset()][column_id_index] = position.value();
        }
      });
    }
    return values;
  };

  auto partition_values = collect_values_of_columns(partition_column_ids);
  auto order_values = collect_values_of_columns(order_column_ids);

  auto function_argument_values = std::vector<AllTypeVariant>(chunk_size, NULL_VALUE);
  if (function_argument_column_id != INVALID_COLUMN_ID) {
    segment_iterate(*chunk.get_segment(function_argument_column_id), [&](const auto& position) {
      if (!position.is_null()) {
        function_argument_values[position.chunk_offset()] = position.value();
      }
    });
  }

  auto result = HashPartitionedData{};

  for (auto chunk_offset = ChunkOffset(0); chunk_offset < chunk_size; ++chunk_offset) {
    auto row_info = RelevantRowInformation{
        .partition_values = std::move(partition_values[chunk_offset]),
        .order_values = std::move(order_values[chunk_offset]),
        .function_argument = std::move(function_argument_values[chunk_offset]),
        .row_id = RowID(chunk_id, chunk_offset),
    };
    const auto hash_partition =
        boost::hash_range(row_info.partition_values.begin(), row_info.partition_values.end()) & hash_partition_mask;
    result[hash_partition].push_back(std::move(row_info));
  }

  return result;
}

void parallel_merge_sort(std::span<RelevantRowInformation> data, auto comparator) {
  const auto base_size = 1u << 17u;

  if (data.size() <= base_size) {
    // NOTE: The "stable" is needed for tests (against sqlite) to pass, but I think that it is not actually required by
    //       the specification.
    std::ranges::stable_sort(data, comparator);
    return;
  }

  const auto mid = data.size() / 2;
  const auto left = data.subspan(0, mid);
  const auto right = data.subspan(mid);

  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{
      std::make_shared<JobTask>([left, &comparator]() { parallel_merge_sort(left, comparator); }),
      std::make_shared<JobTask>([right, &comparator]() { parallel_merge_sort(right, comparator); })};
  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

  std::ranges::inplace_merge(data, data.begin() + static_cast<ssize_t>(mid), comparator);
}

};  // namespace

HashPartitionedData WindowFunctionEvaluator::materialize_into_buckets() const {
  const auto input_table = left_input_table();
  const auto chunk_count = input_table->chunk_count();

  auto tasks = std::vector<std::shared_ptr<AbstractTask>>(chunk_count);
  auto chunk_buckets = std::vector<HashPartitionedData>(chunk_count);

  for (auto chunk_id = ChunkID(0); chunk_id < chunk_count; ++chunk_id) {
    tasks[chunk_id] = std::make_shared<JobTask>([chunk_id, &input_table, &chunk_buckets, this]() {
      const auto chunk = input_table->get_chunk(chunk_id);
      chunk_buckets[chunk_id] = collect_chunk_into_buckets(chunk_id, *chunk, _partition_by_column_ids,
                                                           _order_by_column_ids, _function_argument_column_id);
    });
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

  auto starting_indices = std::vector<PerHash<ssize_t>>(chunk_count + 1);
  for (auto chunk_id = ChunkID(0); chunk_id < chunk_count; ++chunk_id) {
    const auto& my_start = starting_indices[chunk_id];
    auto& next_start = starting_indices[chunk_id + 1];

    for (auto hash_value = 0u; hash_value < hash_partition_partition_count; ++hash_value) {
      next_start[hash_value] = my_start[hash_value] + static_cast<ssize_t>(chunk_buckets[chunk_id][hash_value].size());
    }
  }

  auto output_buckets = HashPartitionedData{};

  for (auto hash_value = 0u; hash_value < hash_partition_partition_count; ++hash_value) {
    output_buckets[hash_value].resize(starting_indices.back()[hash_value]);
  }

  for (auto chunk_id = ChunkID(0); chunk_id < chunk_count; ++chunk_id) {
    tasks[chunk_id] = std::make_shared<JobTask>([chunk_id, &chunk_buckets, &output_buckets, &starting_indices]() {
      for (auto hash_value = 0u; hash_value < hash_partition_partition_count; ++hash_value) {
        auto& my_values = chunk_buckets[chunk_id][hash_value];
        std::ranges::move(my_values, output_buckets[hash_value].begin() + starting_indices[chunk_id][hash_value]);
      }
    });
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

  return output_buckets;
}

void WindowFunctionEvaluator::partition_and_order(HashPartitionedData& buckets) const {
  const auto& sort_modes = dynamic_cast<const WindowExpression&>(*_window_function_expression->window()).sort_modes;
  const auto is_column_reversed = [&sort_modes](const auto column_index) {
    return sort_modes[column_index] == SortMode::Descending;
  };

  const auto comparator = [&is_column_reversed](const RelevantRowInformation& lhs, const RelevantRowInformation& rhs) {
    const auto comp_result = compare_with_null_equal(lhs.partition_values, rhs.partition_values);
    if (std::is_neq(comp_result)) {
      return std::is_lt(comp_result);
    }
    return std::is_lt(compare_with_null_equal(lhs.order_values, rhs.order_values, is_column_reversed));
  };

  spawn_and_wait_per_hash(buckets, [&comparator](auto& bucket) { parallel_merge_sort(bucket, comparator); });
}

template <typename InputColumnType, WindowFunction window_function>
  requires SupportsOnePass<InputColumnType, window_function>
void WindowFunctionEvaluator::compute_window_function_one_pass(const HashPartitionedData& partitioned_data,
                                                               auto&& emit_computed_value) const {
  spawn_and_wait_per_hash(partitioned_data, [&emit_computed_value](const auto& hash_partition) {
    using Traits = WindowFunctionEvaluatorTraits<InputColumnType, window_function>;
    using Impl = typename Traits::OnePassImpl;
    using State = typename Impl::State;
    auto state = State{};

    const RelevantRowInformation* previous_row = nullptr;

    for (const auto& row : hash_partition) {
      if (previous_row && std::is_eq(compare_with_null_equal(previous_row->partition_values, row.partition_values))) {
        Impl::update_state(state, row);
      } else {
        state = Impl::initial_state(row);
      }
      emit_computed_value(row.row_id, Impl::current_value(state));
      previous_row = &row;
    }
  });
}

namespace {

// Sentinel type to indicate that Range mode is used without an order-by column.
struct NoOrderByColumn {};

template <FrameType frame_type, typename OrderByColumnType>
struct WindowBoundCalculator {
  static QueryRange calculate_window_bounds([[maybe_unused]] std::span<const RelevantRowInformation> partition,
                                            [[maybe_unused]] uint64_t tuple_index,
                                            [[maybe_unused]] const FrameDescription& frame) {
    auto error_stream =
        std::ostringstream("Unsupported frame type and order-by column type combination: ", std::ios_base::ate);
    error_stream << [&]() {
      using std::literals::string_view_literals::operator""sv;

      switch (frame_type) {
        case FrameType::Rows:
          return "Rows"sv;
        case FrameType::Range:
          return "Range"sv;
        case FrameType::Groups:
          return "Groups"sv;
      }
      Fail("Invalid FrameType.");
    }();
    error_stream << " ";
    error_stream << data_type_to_string.left.at(data_type_from_type<OrderByColumnType>());
    Fail(error_stream.str());
  }
};

template <typename OrderByColumnType>
struct WindowBoundCalculator<FrameType::Rows, OrderByColumnType> {
  static QueryRange calculate_window_bounds(std::span<const RelevantRowInformation> partition, uint64_t tuple_index,
                                            const FrameDescription& frame) {
    const auto start = frame.start.unbounded ? 0 : clamped_sub<uint64_t>(tuple_index, frame.start.offset, 0);
    const auto end = frame.end.unbounded ? partition.size()
                                         : clamped_add<uint64_t>(tuple_index, frame.end.offset + 1, partition.size());
    return {start, end};
  }
};

template <>
struct WindowBoundCalculator<FrameType::Range, NoOrderByColumn> {
  static QueryRange calculate_window_bounds(std::span<const RelevantRowInformation> partition,
                                            [[maybe_unused]] uint64_t tuple_index,
                                            [[maybe_unused]] const FrameDescription& frame) {
    return {0, partition.size()};
  }
};

template <typename OrderByColumnType>
  requires std::is_integral_v<OrderByColumnType>
struct WindowBoundCalculator<FrameType::Range, OrderByColumnType> {
  static QueryRange calculate_window_bounds(std::span<const RelevantRowInformation> partition, uint64_t tuple_index,
                                            const FrameDescription& frame) {
    const auto current_value = as_optional<OrderByColumnType>(partition[tuple_index].order_values[0]);

    // We sort null-first
    const auto end_of_null_peer_group_it =
        std::ranges::partition_point(partition, [&](const auto& row) { return variant_is_null(row.order_values[0]); });
    const auto end_of_null_peer_group = std::distance(partition.begin(), end_of_null_peer_group_it);
    const auto non_null_values = partition.subspan(end_of_null_peer_group);

    const auto start = [&]() -> uint64_t {
      if (frame.start.unbounded) {
        return 0;
      }
      if (!current_value) {
        return 0;
      }
      const auto it = std::ranges::partition_point(non_null_values, [&](const auto& row) {
        return std::cmp_less(get<OrderByColumnType>(row.order_values[0]) + frame.start.offset, *current_value);
      });
      return end_of_null_peer_group + std::distance(non_null_values.begin(), it);
    }();

    const auto end = [&]() -> uint64_t {
      if (frame.end.unbounded) {
        return partition.size();
      }
      if (!current_value) {
        return end_of_null_peer_group;
      }
      const auto it = std::ranges::partition_point(non_null_values, [&](const auto& row) {
        return std::cmp_less_equal(get<OrderByColumnType>(row.order_values[0]), *current_value + frame.end.offset);
      });
      return end_of_null_peer_group + std::distance(non_null_values.begin(), it);
    }();

    return {start, end};
  }
};

template <typename InputColumnType, WindowFunction window_function, FrameType frame_type, typename OrderByColumnType>
  requires SupportsSegmentTree<InputColumnType, window_function>
void templated_compute_window_function_segment_tree(const HashPartitionedData& partitioned_data,
                                                    const FrameDescription& frame, auto&& emit_computed_value) {
  spawn_and_wait_per_hash(partitioned_data, [&emit_computed_value, &frame](const auto& hash_partition) {
    for_each_partition(hash_partition, [&](uint64_t partition_start, uint64_t partition_end) {
      const auto partition = std::span(hash_partition.begin() + partition_start, partition_end - partition_start);

      using Traits = WindowFunctionEvaluatorTraits<InputColumnType, window_function>;
      using Impl = typename Traits::NullableSegmentTreeImpl;
      using TreeNode = typename Impl::TreeNode;

      const auto partition_size = partition.size();

      auto leaf_values = std::vector<TreeNode>(partition_size);
      std::ranges::transform(partition, leaf_values.begin(), [](const auto& row) {
        return Impl::node_from_value(as_optional<InputColumnType>(row.function_argument));
      });

      const auto combine = [](auto lhs, auto rhs) { return Impl::combine(std::move(lhs), std::move(rhs)); };
      const auto make_neutral_element = []() { return Impl::neutral_element; };
      const auto segment_tree = SegmentTree<TreeNode, decltype(combine), decltype(make_neutral_element)>(
          leaf_values, combine, make_neutral_element);

      for (auto tuple_index = 0u; tuple_index < partition_size; ++tuple_index) {
        using BoundCalculator = WindowBoundCalculator<frame_type, OrderByColumnType>;
        const auto query_range = BoundCalculator::calculate_window_bounds(partition, tuple_index, frame);
        emit_computed_value(partition[tuple_index].row_id,
                            Impl::transform_query(segment_tree.range_query(query_range)));
      }
    });
  });
}

};  // namespace

template <typename InputColumnType, WindowFunction window_function>
  requires SupportsSegmentTree<InputColumnType, window_function>
void WindowFunctionEvaluator::compute_window_function_segment_tree(const HashPartitionedData& partitioned_data,
                                                                   auto&& emit_computed_value) const {
  const auto& frame = frame_description();
  switch (frame.type) {
    case FrameType::Rows:
      templated_compute_window_function_segment_tree<InputColumnType, window_function, FrameType::Rows, void>(
          partitioned_data, frame, emit_computed_value);
      break;
    case FrameType::Range:
      if (_order_by_column_ids.empty()) {
        templated_compute_window_function_segment_tree<InputColumnType, window_function, FrameType::Range,
                                                       NoOrderByColumn>(partitioned_data, frame, emit_computed_value);
      } else {
        resolve_data_type(left_input_table()->column_data_type(_order_by_column_ids[0]), [&](auto data_type) {
          using OrderByColumnType = typename decltype(data_type)::type;
          templated_compute_window_function_segment_tree<InputColumnType, window_function, FrameType::Range,
                                                         OrderByColumnType>(partitioned_data, frame,
                                                                            emit_computed_value);
        });
      }
      break;
    case FrameType::Groups:
      Fail("Unsupported frame type: Groups.");
  }
}

template <typename OutputColumnType>
std::shared_ptr<const Table> WindowFunctionEvaluator::annotate_input_table(
    std::vector<std::pair<pmr_vector<OutputColumnType>, pmr_vector<bool>>> segment_data_for_output_column) const {
  const auto input_table = left_input_table();

  const auto chunk_count = input_table->chunk_count();
  const auto column_count = input_table->column_count();

  const auto new_column_name = _window_function_expression->as_column_name();
  const auto new_column_type = _window_function_expression->data_type();
  const auto new_column_definition = TableColumnDefinition(new_column_name, new_column_type, is_output_nullable());

  // Create value segments for our output column.
  auto value_segments_for_new_column = std::vector<std::shared_ptr<AbstractSegment>>();
  value_segments_for_new_column.reserve(chunk_count);
  for (auto chunk_id = ChunkID(0); chunk_id < chunk_count; ++chunk_id) {
    if (is_output_nullable()) {
      value_segments_for_new_column.emplace_back(
          std::make_shared<ValueSegment<OutputColumnType>>(std::move(segment_data_for_output_column[chunk_id].first),
                                                           std::move(segment_data_for_output_column[chunk_id].second)));
    } else {
      value_segments_for_new_column.emplace_back(
          std::make_shared<ValueSegment<OutputColumnType>>(std::move(segment_data_for_output_column[chunk_id].first)));
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

  // Create output table reusing the segments from input and the newly created segments for the output column.
  auto output_column_definitions = input_table->column_definitions();
  output_column_definitions.push_back(new_column_definition);
  auto output_chunks = std::vector<std::shared_ptr<Chunk>>();
  output_chunks.reserve(chunk_count);
  for (auto chunk_id = ChunkID(0); chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = input_table->get_chunk(chunk_id);
    auto output_segments = Segments();
    for (auto column_id = ColumnID(0); column_id < column_count; ++column_id) {
      output_segments.emplace_back(chunk->get_segment(column_id));
    }
    output_segments.push_back(std::move(outputted_segments_for_new_column[chunk_id]));

    output_chunks.emplace_back(std::make_shared<Chunk>(std::move(output_segments)));
  }

  return std::make_shared<Table>(output_column_definitions, input_table->type(), std::move(output_chunks));
}

std::ostream& operator<<(std::ostream& stream, const ComputationStrategy computation_strategy) {
  return stream << computation_strategy_to_string.left.at(computation_strategy);
}

void WindowFunctionEvaluator::PerformanceData::output_to_stream(std::ostream& stream,
                                                                DescriptionMode description_mode) const {
  OperatorPerformanceData<OperatorSteps>::output_to_stream(stream, description_mode);

  const auto separator = (description_mode == DescriptionMode::SingleLine ? ' ' : '\n');
  stream << separator << "Computation strategy: " << computation_strategy << ".";
}

}  // namespace hyrise::window_function_evaluator
