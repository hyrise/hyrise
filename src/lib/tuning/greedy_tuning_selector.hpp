#pragma once

#include "tuning/abstract_tuning_selector.hpp"
#include "tuning/tuning_operation.hpp"
#include "tuning/tuning_option.hpp"

namespace opossum {

/**
 * The GreedyTuningSelector uses the following algorithm to determine TuningOperations:
 * 1. Sort Tuning Choices by ascending desirability, within same desirability by ascending cost.
 *    Then repeatedly performs the following operations:
 * 2. Tries to free up budget by rejecting choices with lowest desirabilities ("starting from the end of list")
 * 3. Tries to fill unused budget by accepting choices with highest desirabilities ("starting from the start of the list")
 *
 * Also see the example at https://docs.google.com/presentation/d/1iWL86UmbMkUooi-7zyGk-Bb_GfGVPPVLVx4tmKxzKyE/edit?usp=sharing
 * (BYOD "Self Driving Database" final presentation, starting with slide 14)
 *
 */
class GreedyTuningSelector : public AbstractTuningSelector {
 public:
  std::vector<std::shared_ptr<TuningOperation>> select(const std::vector<std::shared_ptr<TuningOption>>& choices,
                                                       float cost_budget) final;
};

}  // namespace opossum
