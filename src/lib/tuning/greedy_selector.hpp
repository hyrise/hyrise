#pragma once

#include "tuning/abstract_selector.hpp"
#include "tuning/tuning_choice.hpp"
#include "tuning/tuning_operation.hpp"

namespace opossum {

/**
 * The GreedySelector uses the following algorithm to determine TuningOperations:
 * 1. Sort Tuning Choices by ascending desirability, within same desirability by ascending cost.
 *    Then repeatedly performs the following operations:
 * 2. Tries to free up budget by rejecting choices with lowest desirabilities ("starting from the end of list")
 * 3. Tries to fill unused budget by accepting choices with highest desirabilities ("starting from the start of the list")
 */
class GreedySelector : public AbstractSelector {
 public:
  std::vector<std::shared_ptr<TuningOperation>> select(const std::vector<std::shared_ptr<TuningChoice>>& choices,
                                                       float cost_budget) final;
};

}  // namespace opossum
