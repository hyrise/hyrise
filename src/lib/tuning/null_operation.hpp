#pragma once

#include <iostream>

#include "tuning_operation.hpp"

namespace opossum {

/**
 * A NullOperation is the null-object-pattern implementation of a TuningOperation.
 * Consequently, it does nothing on execution.
 * It is created as a result of
 * - accepting a TuningChoice that is already chosen (keep the value as is)
 * - rejecting a TuningChoice that is not yet chosen (do not change status quo)
 */
class NullOperation : public TuningOperation {
 public:
  void execute() final;
  void print_on(std::ostream& output) const final;
};

}  // namespace opossum
