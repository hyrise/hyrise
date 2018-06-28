#pragma once

#include <iostream>

namespace opossum {

/**
 * A TuningOperation is created from a TuningOption and either
 * performs or reverts the respective system modification.
 */
class TuningOperation {
 public:
  virtual ~TuningOperation() {}
  /**
   * Perform this tuning operation.
   * If you need a no-op TuningOperation, look at NullTuningOperation.
   */
  virtual void execute() = 0;

  /**
   * Print detailed information on the concrete TuningOperation.
   *
   * The default implementation prints "TuningOperation{}"
   */
  virtual void print_on(std::ostream& output) const;

  friend std::ostream& operator<<(std::ostream& output, const TuningOperation& operation) {
    operation.print_on(output);
    return output;
  }
};

}  // namespace opossum
