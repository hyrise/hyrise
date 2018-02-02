#pragma once

#include "tuning/abstract_selector.hpp"
#include "tuning/tuning_choice.hpp"
#include "tuning/tuning_operation.hpp"

namespace opossum {

/**
 * ToDo(group01): describe specific mechanism to generate operation sequence
 */
class GreedySelector : public AbstractSelector {
 public:
  std::vector<std::shared_ptr<TuningOperation>> select(std::vector<std::shared_ptr<TuningChoice>> choices,
                                                       float memory_budget) final;
};

}  // namespace opossum
