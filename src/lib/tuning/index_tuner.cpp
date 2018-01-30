#include "index_tuner.hpp"

#include <chrono>
#include <limits>

#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_operator.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/storage_manager.hpp"
#include "tuning/index_evaluator.hpp"
#include "tuning/index_selector.hpp"
#include "utils/logging.hpp"

namespace opossum {

IndexTuner::IndexTuner(std::shared_ptr<SystemStatistics> statistics)
    : _statistics{statistics},
      _evaluator{std::make_unique<IndexEvaluator>()},
      _selector{std::make_unique<IndexSelector>()},
      _time_budget{std::numeric_limits<float>::infinity()},
      _memory_budget{std::numeric_limits<float>::infinity()} {}

void IndexTuner::set_time_budget(float budget) { _time_budget = budget; }

void IndexTuner::disable_time_budget() { _time_budget = std::numeric_limits<float>::infinity(); }

float IndexTuner::time_budget() { return _time_budget; }

void IndexTuner::set_memory_budget(float budget) { _memory_budget = budget; }

void IndexTuner::disable_memory_budget() { _memory_budget = std::numeric_limits<float>::infinity(); }

float IndexTuner::memory_budget() { return _memory_budget; }

void IndexTuner::execute() {
  LOG_INFO("Evaluating indices...");
  auto index_evaluations = _evaluator->evaluate_indices(*_statistics);

  LOG_DEBUG("Evaluations:");
  for (auto evaluation : index_evaluations) {
    LOG_DEBUG("-> " << evaluation);
  }

  LOG_INFO("Indices evaluated.");

  LOG_INFO("Selecting indices...");
  const auto& operations = _selector->select_indices(index_evaluations, _memory_budget);
  LOG_INFO("Indices selected.");

  LOG_INFO("Recommended changes:");
  for (const auto& operation : operations) {
    const auto& column_name = StorageManager::get().get_table(operation.table_name)->column_name(operation.column_id);
    if (operation.create) {
      LOG_INFO("  -> Create index on table " << operation.table_name << ", column " << column_name);
    } else {
      LOG_INFO("  -> Delete index on table " << operation.table_name << ", column " << column_name);
    }
  }

  LOG_INFO("Executing planned operations...");
  _execute_operations(operations);
  LOG_INFO("Planned operations executed.");
}

void IndexTuner::_execute_operations(const std::vector<IndexOperation>& operations) {
  std::chrono::duration<float, std::chrono::seconds::period> timeout{_time_budget};
  auto begin = std::chrono::high_resolution_clock::now();

  for (const auto& operation : operations) {
    if (operation.create) {
      LOG_DEBUG("  -> " << operation);
      _create_index(operation.table_name, operation.column_id);
    } else {
      LOG_DEBUG("  -> " << operation);
      _delete_index(operation.table_name, operation.column_id);
    }
    auto end = std::chrono::high_resolution_clock::now();
    if (end - begin > timeout) {
      std::cout << "  IndexTuning was interrupted because the time budget was insufficient\n";
      break;
    }
  }

  // ToDo(group01): Flush query plans from caches that are affected by these index changes
  // ToDo(group01): Maybe even re-build those query plans if we still have time left
}

void IndexTuner::_create_index(const std::string& table_name, ColumnID column_id) {
  auto table = StorageManager::get().get_table(table_name);
  auto chunk_count = table->chunk_count();

  std::vector<ColumnID> column_ids;
  column_ids.emplace_back(column_id);

  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    auto chunk = table->get_chunk(chunk_id);
    // ToDo(group01): Who decides what type of index is created? Is it static config or
    //                is it decided dynamically during runtime?
    chunk->create_index<GroupKeyIndex>(column_ids);
  }
}

void IndexTuner::_delete_index(const std::string& table_name, ColumnID column_id) {
  // ToDo(group01): Currently there seems to be no way to remove an index from a chunk
}

}  // namespace opossum
