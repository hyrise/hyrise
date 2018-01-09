#include "index_tuner.hpp"

#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_operator.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

IndexTuner::IndexTuner(std::shared_ptr<SystemStatistics> statistics)
    : _statistics{statistics}, _heuristic{std::make_unique<IndexSelectionHeuristic>()} {}

void IndexTuner::execute() {
  const auto& proposals = _heuristic->recommend_changes(*_statistics);

  std::cout << "Recommended changes: \n";

  for (const auto& proposal : proposals) {
    const auto& column_name = StorageManager::get().get_table(proposal.table_name)->column_name(proposal.column_id);
    std::cout << "  Create index on table " << proposal.table_name << ", column " << column_name;
    std::cout << " (desirablity " << proposal.desirablility * 100 << "%)\n";
    _create_index(proposal.table_name, proposal.column_id);
  }
}

void IndexTuner::_create_index(const std::string& table_name, const ColumnID& column_id) {
  auto table = StorageManager::get().get_table(table_name);
  auto chunk_count = table->chunk_count();

  std::vector<ColumnID> column_ids;
  column_ids.emplace_back(column_id);

  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    auto chunk = table->get_chunk(chunk_id);
    // ToDo(group01): Who decides what type of index is created? Is it static config or
    //                is it decided dynamically during runtime?
    chunk->create_index<AdaptiveRadixTreeIndex>(column_ids);
      //chunk->create_index<GroupKeyIndex>(column_ids);
  }
}

}  // namespace opossum
