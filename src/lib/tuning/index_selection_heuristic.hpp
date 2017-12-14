#pragma once

#include "tuning/system_statistics.hpp"

namespace opossum {

/**
 * The IndexSelectionHeuristic takes information about the current system
 * (e.g. query plan cache, table statistics) and proposes indices to be created
 * or removed.
 */
class IndexSelectionHeuristic {
 public:
  IndexSelectionHeuristic();

  // Runs the heuristic to analyze the SystemStatistics object and return
  // recommended changes to be made to the live system.
  // TODO(group01) determine return value of this operation
  void recommend_changes(const SystemStatistics& statistics);

 protected:
};

}  // namespace opossum
