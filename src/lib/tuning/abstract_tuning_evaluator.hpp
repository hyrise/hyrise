#pragma once

#include <memory>
#include <vector>

#include "tuning/tuning_choice.hpp"

namespace opossum {

/**
 * An AbstractTuningEvaluator analyzes the current system state and proposes an
 * unordered list of TuningChoices that might improve the system performance.
 */
class AbstractTuningEvaluator {
 public:
  virtual ~AbstractTuningEvaluator() {}
  /**
   * Generate TuningChoices and append them to the given vector.
   */
  virtual void evaluate(std::vector<std::shared_ptr<TuningChoice>>& choices) = 0;
};

}  // namespace opossum
