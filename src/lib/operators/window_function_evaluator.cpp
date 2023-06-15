#include <algorithm>
#include <array>
#include <utility>

#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/value_segment.hpp"
#include "window_function_evaluator.hpp"

namespace hyrise {

const std::string& WindowFunctionEvaluator::name() const {
  static const auto name = std::string{"WindowFunctionEvaluator"};
  return name;
}

std::shared_ptr<const Table> WindowFunctionEvaluator::_on_execute() {
  auto partitioned_data = partition_and_sort();

  std::shared_ptr<const Table> result;

  resolve_data_type(_window_function_expression.data_type(), [&](auto type) {
    using T = typename decltype(type)::type;

    std::vector<ValueSegment<T>> result_segments(left_input_table()->chunk_count());
    compute_window_function<T>(partitioned_data, [&](RowID row_id, AllTypeVariant computed_value) {
      result_segments[row_id.chunk_id][row_id.chunk_offset] = std::move(computed_value);
    });

    result = annotate_input_table(std::move(result_segments));
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

using HashPartitionedData =
    std::array<std::vector<RelevantRowInformation>, WindowFunctionEvaluator::hash_partition_partition_count>;

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

WindowFunctionEvaluator::PartitionedData WindowFunctionEvaluator::partition_and_sort() const {
  const auto input_table = left_input_table();
  const auto column_count = input_table->column_count();
  const auto hash_partitioned_data =
      parallel_merge_sort(*input_table, ChunkRange{.start = ChunkID(0), .end = input_table->chunk_count()},
                          _partition_by_column_id, _order_by_column_id);

  auto result = PartitionedData{};
  for (const auto& hash_partition : hash_partitioned_data) {
    for (const auto& row_info : hash_partition) {
      // TODO(group): Don't include all cells here, but figure out what to keep in RelevantRowInformation and return that
      auto row_values = std::vector<AllTypeVariant>{};
      // TODO(group): Add Table::get_row(RowID)
      const auto chunk = input_table->get_chunk(row_info.row_id.chunk_id);

      for (auto column_id = ColumnID(0); column_id < column_count; ++column_id) {
        auto segment = chunk->get_segment(column_id);
        row_values.emplace_back((*segment)[row_info.row_id.chunk_offset]);
      }

      result.push_back({std::move(row_values), row_info.row_id});
    }
  }
  return result;
}

void WindowFunctionEvaluator::compute_window_function(const PartitionedData& partitioned_data,
                                                      auto&& emit_computed_value) const {
  (void)partitioned_data;
  (void)emit_computed_value;
  Fail("unimplemented");
}

template <typename T>
std::shared_ptr<const Table> WindowFunctionEvaluator::annotate_input_table(
    std::vector<ValueSegment<T>> segments_for_output_column) const {
  (void)segments_for_output_column;
  Fail("Unimplemented");
}

}  // namespace hyrise
