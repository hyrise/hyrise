#include "index_tuner.hpp"

#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_operator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

IndexTuner::IndexTuner() : _heuristic{std::make_unique<IndexSelectionHeuristic>()} {}

void IndexTuner::execute() {
  // const auto& query_plan_cache = SQLQueryOperator::get_query_plan_cache();
}

}  // namespace opossum
