#pragma once

#include <memory>

#include "tuning/index_evaluation.hpp"
#include "tuning/system_statistics.hpp"

namespace opossum {

/**
 * An AbstractIndexEvaluator takes information about the current system
 * (e.g. query plan cache, table statistics) and proposes indices to be created
 * or removed.
 */
class AbstractIndexEvaluator {
 public:
  /**
   * Analyzes the system as represented by the SystemStatistics object and
   * returns a list of IndexEvaluations for indices considered relevant
   */
  virtual std::vector<IndexEvaluation> evaluate_indices(const SystemStatistics& statistics) = 0;
};

}  // namespace opossum
