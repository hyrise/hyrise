#include "system_statistics.hpp"

#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_operator.hpp"

namespace opossum {

SystemStatistics::SystemStatistics(const SQLQueryCache<std::shared_ptr<SQLQueryPlan>>& cache)
    : _recent_queries{}, _cache(cache) {}

const SQLQueryCache<std::shared_ptr<SQLQueryPlan>>& SystemStatistics::cache() const { return _cache; }

}  // namespace opossum
