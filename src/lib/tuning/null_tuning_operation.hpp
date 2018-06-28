#pragma once

#include <iostream>

#include "tuning_operation.hpp"

namespace opossum {

/**
 * A NullTuningOperation is the null-object-pattern implementation of a TuningOperation.
 * Consequently, it does nothing on execution.
 * It is created as a result of
 * - accepting a TuningOption that is already chosen (keep the value as is)
 * - rejecting a TuningOption that is not yet chosen (do not change status quo)
 */
class NullTuningOperation : public TuningOperation {
 public:
  void execute() final;
  void print_on(std::ostream& output) const final;
};

}  // namespace opossum
