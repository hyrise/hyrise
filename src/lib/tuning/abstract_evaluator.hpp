#pragma once

#include <memory>
#include <vector>

#include "tuning/tuning_choice.hpp"

namespace opossum {

/**
 * An AbstractEvaluator analyzes the current system state and proposes a list
 * of TuningChoices that might improve the system performance.
 */
class AbstractEvaluator {
 public:
  /**
   * Generate TuningChoices and append them to the given vector.
   */
  virtual void evaluate(std::vector<std::shared_ptr<TuningChoice>>& choices) = 0;
};

}  // namespace opossum
