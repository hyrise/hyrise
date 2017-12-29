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
  // TODO(group01) retrieve query cache from system-wide singleton as soon as that exists
  explicit SystemStatistics(const SQLQueryCache<SQLQueryPlan>& cache);

  // Retrieves recent query plans from the currently active query cache implementation
  // TODO(group01) copying SQLQueryPlan instances is expensive, can we obtain shared_ptrs?
  const std::vector<SQLQueryPlan>& recent_queries() const;

 protected:
  mutable std::vector<SQLQueryPlan> _recent_queries;
  const SQLQueryCache<SQLQueryPlan>& _cache;
};

}  // namespace opossum
