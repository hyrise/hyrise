#include "chunk_select.hpp"

#include <cmath>
#include <memory>
#include <thread>
#include <span>
#include <vector>

#include "hyrise.hpp"
#include "operators/abstract_operator.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/print.hpp"
#include "operators/projection.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/task_queue.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "utils/assert.hpp"
#include "utils/progressive_utils.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"

namespace hyrise {

ChunkSelect::ChunkSelect(const std::shared_ptr<const AbstractOperator>& input_operator, std::vector<ColumnID>&& column_ids,
                         std::vector<uint8_t>&& partition_counts, std::vector<uint16_t>&& hash_values)
    : AbstractReadOnlyOperator(OperatorType::ChunkSelect, input_operator, nullptr),
      _column_ids{std::move(column_ids)},
      _partition_counts{std::move(partition_counts)},
      _hash_values{std::move(hash_values)} {
  Assert(_column_ids.size() == _partition_counts.size(), "Unexpected configuration.");
  Assert(_column_ids.size() == _hash_values.size(), "Unexpected configuration.");

  for (const auto& partition_count : _partition_counts) {
    Assert(std::log2(partition_count) == std::ceil(std::log2(partition_count)), "Partition sizes must be a power of two.");
  }
}

const std::string& ChunkSelect::name() const {
  static const auto name = std::string{"ChunkSelect"};
  return name;
}

std::shared_ptr<AbstractOperator> ChunkSelect::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  auto copy_column_ids = _column_ids;
  auto copy_partition_counts = _partition_counts;
  auto copy_hash_values = _hash_values;
  auto copy = std::make_shared<ChunkSelect>(copied_left_input, std::move(copy_column_ids), std::move(copy_partition_counts), std::move(copy_hash_values));
  return copy;
}

void ChunkSelect::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> ChunkSelect::_on_execute() {
  auto timer = Timer{};
  const auto& input_table = left_input_table();
  // const auto input_table_chunk_count = input_table->chunk_count();
  // const auto partition_column_count = _column_ids.size();
  Assert(input_table->type() == TableType::References, "ChunkSelect cannot be executed on data tables.");

  return input_table;
}

}  // namespace hyrise
