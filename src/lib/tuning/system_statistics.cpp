#include "system_statistics.hpp"

namespace opossum {

SystemStatistics::SystemStatistics() : _recent_queries{} {}

const std::vector<SQLQueryPlan>& SystemStatistics::recent_queries() const {
  // TODO(group01) implement
  // const auto& query_plan_cache = SQLQueryOperator::get_query_plan_cache();
  return _recent_queries;
}

}  // namespace opossum
