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
  std::vector<std::shared_ptr<TuningOperation>> select(const std::vector<std::shared_ptr<TuningChoice>>& choices,
                                                       float cost_budget) final;
};

}  // namespace opossum
