#pragma once

#include <memory>
#include <vector>

#include "sql/sql_query_plan.hpp"

namespace opossum {

/**
 * Contains statistics of the currently running system as a consistent interface
 * to be used by IndexSelectionHeuristic instances.
 */
class SystemStatistics {
 public:
  SystemStatistics();

  // Retrieves recent query plans from the currently active query cache implementation
  // TODO(group01) copying SQLQueryPlan instances is expensive, can we obtain shared_ptrs?
  const std::vector<SQLQueryPlan>& recent_queries() const;

 protected:
  std::vector<SQLQueryPlan> _recent_queries;
};

}  // namespace opossum
