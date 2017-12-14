#include "index_tuner.hpp"

#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_operator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

IndexTuner::IndexTuner()
    : _statistics{std::make_unique<SystemStatistics>()}, _heuristic{std::make_unique<IndexSelectionHeuristic>()} {}

void IndexTuner::execute() { _heuristic->recommend_changes(*_statistics); }

}  // namespace opossum
