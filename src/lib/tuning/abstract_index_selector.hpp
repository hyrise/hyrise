#pragma once

#include <string>

#include "tuning/index_evaluation.hpp"
#include "tuning/index_operation.hpp"

namespace opossum {

/**
 * An AbstractIndexSelector transforms an unordered list of IndexEvaluations into
 * a concrete Sequence of creation and deletion operations.
 *
 * It thereby considers a memory budget that the entire operation sequence as well
 * as any continuous subsequence from the beginning must not exceed.
 * The operation sequence is prioritized by expected performance impact on the system,
 * so that the most beneficial operations (and their dependencies) come before less
 * useful operations.
 */
class AbstractIndexSelector {
 public:
  /**
   * Determine the index operation sequence as specified above.
   */
  virtual std::vector<IndexOperation> select_indices(std::vector<IndexEvaluation> proposals, float memory_budget) = 0;

 protected:
  std::vector<IndexOperation> _operations;
};

}  // namespace opossum
