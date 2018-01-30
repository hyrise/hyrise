#pragma once

#include <memory>
#include <vector>

#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_plan.hpp"

namespace opossum {

/**
 * Contains statistics of the currently running system as a consistent interface
 * to be used by IndexSelectionHeuristic instances.
 */
class SystemStatistics {
 public:
  class SQLQueryCacheEntry {
   public:
    std::string query;
    std::shared_ptr<SQLQueryPlan> query_plan;
    size_t access_frequency;
  };

  // TODO(group01) retrieve query cache from system-wide singleton as soon as that exists
  explicit SystemStatistics(const SQLQueryCache<std::shared_ptr<SQLQueryPlan>>& cache);

  const SQLQueryCache<std::shared_ptr<SQLQueryPlan>>& cache() const;

  // Retrieves recent query plans from the currently active query cache implementation
  const std::vector<SQLQueryCacheEntry>& recent_queries() const;

 protected:
  mutable std::vector<SQLQueryCacheEntry> _recent_queries;
  const SQLQueryCache<std::shared_ptr<SQLQueryPlan>>& _cache;
};

}  // namespace opossum
