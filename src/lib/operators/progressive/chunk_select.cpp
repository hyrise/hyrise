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
#include "operators/progressive/shuffle.hpp"
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

ChunkSelect::ChunkSelect(const std::shared_ptr<const AbstractOperator>& input_operator, std::vector<std::optional<uint16_t>>&& radix_values)
    : AbstractReadOnlyOperator(OperatorType::ChunkSelect, input_operator, nullptr),
      _radix_values{std::move(radix_values)} {
  Assert(input_operator->type() == OperatorType::Shuffle, "Expecting Shuffle as input operator.");

  const auto& shuffle_operator = static_cast<const Shuffle&>(*input_operator);
  _column_ids = shuffle_operator.column_ids();
  _partition_counts = shuffle_operator.partition_counts();

  if (HYRISE_DEBUG) {
    Assert(std::any_of(_radix_values.cbegin(), _radix_values.cend(), [](const auto& item) {
      return item.has_value();
    }), "At least one radix needs to be set.");

    for (auto partition_id = size_t{0}; partition_id < _partition_counts.size(); ++partition_id) {
      if (!_radix_values[partition_id]) {
        continue;
      }
      Assert(_radix_values[partition_id] < _partition_counts[partition_id], "Radix value is too large.");
    }
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
  auto copy_radix_values = _radix_values;
  auto copy = std::make_shared<ChunkSelect>(copied_left_input, std::move(copy_radix_values));
  return copy;
}

void ChunkSelect::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> ChunkSelect::_on_execute() {
  auto timer = Timer{};
  const auto& input_table = left_input_table();
  Assert(input_table->type() == TableType::References, "ChunkSelect cannot be executed on data tables.");

  const auto chunk_count = input_table->chunk_count();
  auto chunk_ids_to_emit = std::vector<ChunkID>{};
  chunk_ids_to_emit.reserve(chunk_count / 2);

  for ()

  
  auto chunks = std::vector<std::shared_ptr<Chunk>>{};
  chunks.reserve(chunk_count / 2);
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    if (!chunk) {
      continue;
    }
    chunks.emplace_back(progressive::recreate_non_const_chunk(chunk));
  }

  return std::make_shared<Table>(input_table->column_definitions(), TableType::References, chunks);
}

}  // namespace hyrise
