#include "index_tuner.hpp"

#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_operator.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

IndexTuner::IndexTuner(std::shared_ptr<SystemStatistics> statistics)
    : _statistics{statistics},
      _evaluator{std::make_unique<IndexEvaluator>()},
      _selector{std::make_unique<IndexSelector>()} {}

void IndexTuner::execute() {
  const auto& operations = _selector->select_indices(_evaluator->evaluate_indices(*_statistics), 0.0f);

  std::cout << "Recommended changes: \n";

  for (const auto& operation : operations) {
    const auto& column_name = StorageManager::get().get_table(operation.table_name)->column_name(operation.column_id);
    if (operation.create) {
      std::cout << "  Create index on table " << operation.table_name << ", column " << column_name << "\n";
      _create_index(operation.table_name, operation.column_id);
    } else {
      std::cout << "  Delete index on table " << operation.table_name << ", column " << column_name << "\n";
      _delete_index(operation.table_name, operation.column_id);
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
  //ToDo(group01): Currently there seems to be no way to remove an index from a chunk
}

}  // namespace opossum
