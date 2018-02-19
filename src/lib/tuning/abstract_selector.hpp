#pragma once

#include <memory>
#include <vector>

#include "tuning/tuning_choice.hpp"
#include "tuning/tuning_operation.hpp"

namespace opossum {

/**
 * An AbstractSelector transforms an unordered list of TuningChoices into
 * a concrete sequence of TuningOperations.
 *
 * It considers a cost budget that the entire operation sequence as well as any
 * continuous subsequence from the beginning must not exceed.
 * The operation sequence is prioritized by expected performance impact on the system,
 * so that the most beneficial operations come before less useful operations.
 *
 * For more information on the underlying problem, see e.g. Wikipedia:
 *   https://en.wikipedia.org/wiki/Knapsack_problem
 */
class AbstractSelector {
 public:
  /**
   * Determine the index operation sequence as specified above based on the
   * given list of choices and the budget value.
   */
  virtual std::vector<std::shared_ptr<TuningOperation>> select(
      const std::vector<std::shared_ptr<TuningChoice>>& choices, float budget) = 0;
};

}  // namespace opossum
