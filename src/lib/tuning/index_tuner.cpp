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
  const auto& operations = _selector->select_indices(_evaluator->evaluate_indices(*_statistics), _memory_budget);

  std::cout << "Recommended changes: \n";
  for (const auto& operation : operations) {
    const auto& column_name = StorageManager::get().get_table(operation.table_name)->column_name(operation.column_id);
    if (operation.create) {
      std::cout << "  Create index on table " << operation.table_name << ", column " << column_name << "\n";
    } else {
      std::cout << "  Delete index on table " << operation.table_name << ", column " << column_name << "\n";
    }
  }

  _execute_operations(operations);
}

void IndexTuner::_execute_operations(const std::vector<IndexOperation>& operations) {
  std::chrono::duration<float, std::chrono::seconds::period> timeout{_time_budget};
  auto begin = std::chrono::high_resolution_clock::now();

  for (const auto& operation : operations) {
    if (operation.create) {
      _create_index(operation.table_name, operation.column_id);
    } else {
      _delete_index(operation.table_name, operation.column_id);
    }
    auto end = std::chrono::high_resolution_clock::now();
    if (end - begin > timeout) {
      std::cout << "  IndexTuning was interrupted because the time budget was insufficient\n";
      break;
    }
  }
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
