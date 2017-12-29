#pragma once

#include <memory>

#include "tuning/index_selection_heuristic.hpp"
#include "tuning/system_statistics.hpp"

namespace opossum {

/**
 * The IndexTuner creates indices based on the current state of the system in
 * order to optimize query performance.
 * It analyzes the query plan cache and table statistics in order to
 * choose appropriate indices using pluggable IndexSelectionHeuristic instances.
 */
class IndexTuner {
 public:
  explicit IndexTuner(std::shared_ptr<SystemStatistics> statistics);

  void execute();

 protected:
  std::shared_ptr<SystemStatistics> _statistics;
  std::unique_ptr<IndexSelectionHeuristic> _heuristic;
};

}  // namespace opossum
