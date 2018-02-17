#pragma once

#include <iostream>

#include "tuning_operation.hpp"

namespace opossum {

class NullOperation : public TuningOperation {
 public:
  void execute() final;
  void print_on(std::ostream& output) const final;
};

}  // namespace opossum
