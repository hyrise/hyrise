#include <algorithm>
#include <array>
#include <type_traits>
#include <utility>

#include "expression/lqp_column_expression.hpp"
#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "window_function_evaluator.hpp"

namespace hyrise {

WindowFunctionEvaluator::WindowFunctionEvaluator(
    const std::shared_ptr<const AbstractOperator>& input_operator, ColumnID init_partition_by_column_id,
    ColumnID init_order_by_column_id, std::shared_ptr<WindowFunctionExpression> init_window_funtion_expression)
    : AbstractReadOnlyOperator(OperatorType::WindowFunction, input_operator),
      _partition_by_column_id(init_partition_by_column_id),
      _order_by_column_id(init_order_by_column_id),
      _window_function_expression(std::move(init_window_funtion_expression)) {}

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

  compute_window_function<InputColumnType, window_function>(
      partitioned_data, [&](RowID row_id, std::optional<OutputColumnType> computed_value) {
        if (computed_value) {
          segment_data_for_output_column[row_id.chunk_id].first[row_id.chunk_offset] = std::move(*computed_value);
        } else {
          segment_data_for_output_column[row_id.chunk_id].second[row_id.chunk_offset] = true;
        }
      });

  return annotate_input_table(std::move(segment_data_for_output_column));
}

std::shared_ptr<const Table> WindowFunctionEvaluator::_on_execute() {
  auto result = std::shared_ptr<const Table>();
  switch (_window_function_expression->window_function) {
    case WindowFunction::Rank:
      return _templated_on_execute<NullValue, WindowFunction::Rank>();
    case WindowFunction::Sum:
      resolve_data_type(_window_function_expression->argument()->data_type(), [&](auto input_data_type) {
        using InputColumnType = typename decltype(input_data_type)::type;
        if constexpr (std::is_arithmetic_v<InputColumnType>) {
          result = _templated_on_execute<InputColumnType, WindowFunction::Sum>();
        } else {
          Fail("Unsupported input column type for sum.");
        }
      });
      return result;
    default:
      Fail("Unsupported WindowFunction.");
  }
}

namespace {

struct RelevantRowInformation {
  AllTypeVariant partition_value;
  AllTypeVariant order_value;
  RowID row_id;

  static bool compare_for_hash_partitioning(const RelevantRowInformation& lhs, const RelevantRowInformation& rhs) {
    if (lhs.partition_value != rhs.partition_value)
      return lhs.partition_value < rhs.partition_value;
    return lhs.order_value < rhs.order_value;
  }
};

using HashPartitionedData = WindowFunctionEvaluator::PerHash<std::vector<RelevantRowInformation>>;

HashPartitionedData partition_and_sort_chunk(const Chunk& chunk, ChunkID chunk_id, ColumnID partition_column_id,
                                             ColumnID order_column_id) {
  auto result = HashPartitionedData{};

  const auto partition_segment = chunk.get_segment(partition_column_id);
  const auto order_segment = chunk.get_segment(order_column_id);

  for (auto chunk_offset = ChunkOffset(0), row_count = chunk.size(); chunk_offset < row_count; ++chunk_offset) {
    auto row_info = RelevantRowInformation{
        .partition_value = (*partition_segment)[chunk_offset],
        .order_value = (*order_segment)[chunk_offset],
        .row_id = RowID(chunk_id, chunk_offset),
    };
    const auto hash_partition =
        std::hash<AllTypeVariant>()(row_info.partition_value) & WindowFunctionEvaluator::hash_partition_mask;
    result[hash_partition].push_back(std::move(row_info));
  }

  for (auto& partition : result) {
    std::ranges::sort(partition, RelevantRowInformation::compare_for_hash_partitioning);
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

HashPartitionedData parallel_merge_sort(const Table& input_table, ChunkRange chunk_range, ColumnID partition_column_id,
                                        ColumnID order_column_id) {
  if (chunk_range.is_single_chunk()) {
    const auto chunk_id = chunk_range.start;
    const auto chunk = input_table.get_chunk(chunk_id);
    return partition_and_sort_chunk(*chunk, chunk_id, partition_column_id, order_column_id);
  }

  const auto middle = ChunkID(chunk_range.start + chunk_range.size() / 2);
  auto left_result = HashPartitionedData{};
  auto right_result = HashPartitionedData{};
  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
  tasks.emplace_back(std::make_shared<JobTask>([&]() {
    left_result = parallel_merge_sort(input_table, ChunkRange{.start = chunk_range.start, .end = middle},
                                      partition_column_id, order_column_id);
  }));
  tasks.emplace_back(std::make_shared<JobTask>([&]() {
    right_result = parallel_merge_sort(input_table, ChunkRange{.start = middle, .end = chunk_range.end},
                                       partition_column_id, order_column_id);
  }));

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

  auto result = HashPartitionedData{};

  for (auto hash_value = 0u; hash_value < WindowFunctionEvaluator::hash_partition_partition_count; ++hash_value) {
    const auto& left = left_result[hash_value];
    const auto& right = right_result[hash_value];
    auto& merged = result[hash_value];
    merged.resize(left.size() + right.size());
    std::ranges::merge(left, right, merged.begin(), RelevantRowInformation::compare_for_hash_partitioning);
  }

  return result;
}

}  // namespace

WindowFunctionEvaluator::PerHash<WindowFunctionEvaluator::PartitionedData> WindowFunctionEvaluator::partition_and_sort()
    const {
  const auto input_table = left_input_table();
  const auto column_count = input_table->column_count();
  const auto hash_partitioned_data =
      parallel_merge_sort(*input_table, ChunkRange{.start = ChunkID(0), .end = input_table->chunk_count()},
                          _partition_by_column_id, _order_by_column_id);

  auto result = PerHash<PartitionedData>{};
  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};

  for (auto hash_value = 0u; hash_value < hash_partition_partition_count; ++hash_value) {
    tasks.emplace_back(
        std::make_shared<JobTask>([&hash_partitioned_data, &input_table, &result, column_count, hash_value]() {
          for (const auto& row_info : hash_partitioned_data[hash_value]) {
            // TODO(group): Don't include all cells here, but figure out what to keep in RelevantRowInformation and return that
            auto row_values = std::vector<AllTypeVariant>{};
            // TODO(group): Add Table::get_row(RowID)
            const auto chunk = input_table->get_chunk(row_info.row_id.chunk_id);

            for (auto column_id = ColumnID(0); column_id < column_count; ++column_id) {
              auto segment = chunk->get_segment(column_id);
              row_values.emplace_back((*segment)[row_info.row_id.chunk_offset]);
            }

            result[hash_value].push_back({std::move(row_values), row_info.row_id});
          }
        }));
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);

  return result;
}

template <typename InputColumnType, WindowFunction window_function>
void WindowFunctionEvaluator::compute_window_function(const PerHash<PartitionedData>& partitioned_data,
                                                      auto&& emit_computed_value) const {
  const auto& frame = frame_description();
  using OutputColumnType = typename WindowFunctionTraits<InputColumnType, window_function>::ReturnType;

  const auto spawn_and_wait_per_hash = [&partitioned_data](auto&& per_hash_partition_function) {
    auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
    for (auto hash_value = 0u; hash_value < hash_partition_partition_count; ++hash_value) {
      tasks.emplace_back(std::make_shared<JobTask>([hash_value, &partitioned_data, &per_hash_partition_function]() {
        per_hash_partition_function(partitioned_data[hash_value]);
      }));
    }

    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
  };

  if constexpr (window_function == WindowFunction::Rank) {
    Assert(frame.type == FrameType::Range && frame.start.unbounded && frame.start.type == FrameBoundType::Preceding &&
               !frame.end.unbounded && frame.end.type == FrameBoundType::CurrentRow && frame.end.offset == 0,
           "Rank has FrameBounds unbounded Preceding and 0 Current Row.");

    spawn_and_wait_per_hash([&emit_computed_value, this](const auto& hash_partition) {
      auto current_rank = initial_rank;
      const AllTypeVariant* previous_partition_value = nullptr;

      for (const auto& [row_values, row_id] : hash_partition) {
        if (previous_partition_value && row_values[_partition_by_column_id] != *previous_partition_value)
          current_rank = initial_rank;
        emit_computed_value(row_id, std::make_optional(current_rank++));
        previous_partition_value = &row_values[_partition_by_column_id];
      }
    });
  } else if constexpr (window_function == WindowFunction::Sum) {
    Assert(frame.type == FrameType::Rows && frame.start.unbounded && frame.start.type == FrameBoundType::Preceding &&
               !frame.end.unbounded && frame.end.type == FrameBoundType::CurrentRow && frame.end.offset == 0,
           "Sum has FrameBounds unbounded Preceding and 0 Current Row.");

    spawn_and_wait_per_hash([&emit_computed_value, this](const auto& hash_partition) {
      const auto sum_column_expression =
          std::dynamic_pointer_cast<LQPColumnExpression>(_window_function_expression->argument());
      Assert(sum_column_expression, "Can only sum over single columns.");
      const auto sum_column_id = sum_column_expression->original_column_id;

      // Assume that we calculate standard prefix sum
      const auto zero = static_cast<OutputColumnType>(0);
      auto sum = std::optional<OutputColumnType>();

      const AllTypeVariant* previous_partition_value = nullptr;

      for (const auto& [row_values, row_id] : hash_partition) {
        if (previous_partition_value && row_values[_partition_by_column_id] != *previous_partition_value) {
          sum = std::nullopt;
        }

        if (!variant_is_null(row_values[sum_column_id])) {
          if (!sum) {
            sum = zero;
          }
          *sum += static_cast<OutputColumnType>(get<InputColumnType>(row_values[sum_column_id]));
        }

        emit_computed_value(row_id, sum);
        previous_partition_value = &row_values[_partition_by_column_id];
      }
    });
  } else {
    Fail("Unsupported WindowFunction template instantiation.");
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
  const auto new_column_definition = TableColumnDefinition(new_column_name, new_column_type,
                                                           _window_function_expression->is_nullable_on_lqp(*lqp_node));

  // Create value segments for our output column.
  std::vector<std::shared_ptr<AbstractSegment>> value_segments_for_new_column;
  value_segments_for_new_column.reserve(chunk_count);
  for (auto chunk_id = ChunkID(0); chunk_id < chunk_count; ++chunk_id) {
    value_segments_for_new_column.emplace_back(
        std::make_shared<ValueSegment<T>>(std::move(segment_data_for_output_column[chunk_id].first),
                                          std::move(segment_data_for_output_column[chunk_id].second)));
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
