#pragma once

#include <memory>
#include <vector>

#include "tuning/tuning_option.hpp"

namespace opossum {

/**
 * An AbstractTuningEvaluator analyzes the current system state and proposes an
 * unordered list of TuningOptions that might improve the system performance.
 */
class AbstractTuningEvaluator {
 public:
  virtual ~AbstractTuningEvaluator() {}
  /**
   * Generate TuningOptions and append them to the given vector.
   */
  virtual void evaluate(std::vector<std::shared_ptr<TuningOption>>& choices) = 0;
};

}  // namespace opossum
