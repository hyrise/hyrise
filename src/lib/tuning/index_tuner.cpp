#include "index_tuner.hpp"

#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_operator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

IndexTuner::IndexTuner(std::shared_ptr<SystemStatistics> statistics)
    : _statistics{statistics}, _heuristic{std::make_unique<IndexSelectionHeuristic>()} {}

void IndexTuner::execute() {
  const auto& proposals = _heuristic->recommend_changes(*_statistics);

  std::cout << "Recommended changes: \n";

  for (const auto& proposal : proposals) {
    const auto& column_name = StorageManager::get().get_table(proposal.table_name)->column_name(proposal.column_id);
    std::cout << "Create index on table " << proposal.table_name << ", column " << column_name << "\n";
  }
}

}  // namespace opossum
