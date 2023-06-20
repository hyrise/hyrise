#include <algorithm>
#include <array>
#include <type_traits>
#include <utility>

#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "window_function_evaluator.hpp"

namespace hyrise {

const std::string& WindowFunctionEvaluator::name() const {
  static const auto name = std::string{"WindowFunctionEvaluator"};
  return name;
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

std::shared_ptr<const Table> WindowFunctionEvaluator::_on_execute() {
  const auto input_table = left_input_table();
  const auto chunk_count = input_table->chunk_count();
  auto partitioned_data = partition_and_sort();

  std::shared_ptr<const Table> result;

  resolve_data_type(_window_function_expression.data_type(), [&](auto type) {
    using T = typename decltype(type)::type;

    std::vector<std::pair<pmr_vector<T>, pmr_vector<bool>>> segment_data_for_output_column(chunk_count);

    for (auto chunk_id = ChunkID(0); chunk_id < chunk_count; ++chunk_id) {
      const auto output_length = input_table->get_chunk(chunk_id)->size();
      segment_data_for_output_column[chunk_id].first.resize(output_length);
      segment_data_for_output_column[chunk_id].second.resize(output_length);
    }

    compute_window_function<T>(partitioned_data, [&](RowID row_id, std::optional<T> computed_value) {
      if (computed_value) {
        segment_data_for_output_column[row_id.chunk_id].first[row_id.chunk_offset] = std::move(*computed_value);
      } else {
        segment_data_for_output_column[row_id.chunk_id].second[row_id.chunk_offset] = true;
      }
    });

    result = annotate_input_table(std::move(segment_data_for_output_column));
  });

  return result;
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

template <typename T>
void WindowFunctionEvaluator::compute_window_function(const PerHash<PartitionedData>& partitioned_data,
                                                      auto&& emit_computed_value) const {
  Assert(_window_function_expression.window_function == WindowFunction::Rank,
         "Only WindowFunction::Rank is supported.");

  if constexpr (std::is_integral_v<T>) {
    auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
    for (auto hash_value = 0u; hash_value < hash_partition_partition_count; ++hash_value) {
      tasks.emplace_back(std::make_shared<JobTask>([&partitioned_data, &emit_computed_value, hash_value, this]() {
        T current_rank = initial_rank;
        const AllTypeVariant* previous_partition_value = nullptr;

        for (const auto& [row_values, row_id] : partitioned_data[hash_value]) {
          if (previous_partition_value && row_values[_partition_by_column_id] != *previous_partition_value)
            current_rank = initial_rank;
          emit_computed_value(row_id, current_rank++);
          previous_partition_value = &row_values[_partition_by_column_id];
        }
      }));
    }

    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
  }
}

template <typename T>
std::shared_ptr<const Table> WindowFunctionEvaluator::annotate_input_table(
    std::vector<std::pair<pmr_vector<T>, pmr_vector<bool>>> segment_data_for_output_column) const {
  const auto input_table = left_input_table();

  auto output_column_definitions = input_table->column_definitions();
  output_column_definitions.emplace_back("Rank", DataType::Long, false);

  const auto chunk_count = input_table->chunk_count();
  const auto column_count = input_table->column_count();
  std::vector<std::shared_ptr<Chunk>> output_chunks;
  output_chunks.reserve(chunk_count);

  for (auto chunk_id = ChunkID(0); chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = input_table->get_chunk(chunk_id);
    Segments output_segments;
    for (auto column_id = ColumnID(0); column_id < column_count; ++column_id) {
      output_segments.emplace_back(chunk->get_segment(column_id));
    }
    output_segments.emplace_back(
        std::make_shared<ValueSegment<T>>(std::move(segment_data_for_output_column[chunk_id].first),
                                          std::move(segment_data_for_output_column[chunk_id].second)));

    output_chunks.emplace_back(std::make_shared<Chunk>(std::move(output_segments)));
  }

  return std::make_shared<Table>(output_column_definitions, input_table->type(), std::move(output_chunks));
}

}  // namespace hyrise
