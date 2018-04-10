#pragma once

#include <memory>
#include <vector>

#include "tuning/tuning_choice.hpp"
#include "tuning/tuning_operation.hpp"

namespace opossum {

/**
 * An AbstractTuningSelector transforms an unordered list of TuningChoices into
 * a concrete sequence of TuningOperations.
 *
 * It considers a cost budget that the entire operation sequence as well as any
 * continuous subsequence from the beginning must not exceed.
 * The operation sequence is prioritized by the expected performance improvement
 * (desirability) on the system, so that the most beneficial operations come
 * before less useful operations.
 *
 * The underlying problem relates to the knapsack problem, but goes beyond that
 * in scope (e.g. choices invalidating other choices).
 * For more information, see e.g. Wikipedia:
 *   https://en.wikipedia.org/wiki/Knapsack_problem
 */
class AbstractTuningSelector {
 public:
  virtual ~AbstractTuningSelector() {}
  /**
   * Determine the tuning operation sequence as specified above based on the
   * given list of choices and the budget value.
   */
  virtual std::vector<std::shared_ptr<TuningOperation>> select(
      const std::vector<std::shared_ptr<TuningChoice>>& choices, float budget) = 0;
};

}  // namespace opossum
