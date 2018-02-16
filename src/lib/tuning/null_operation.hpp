#pragma once

#include <iostream>

#include "tuning_operation.hpp"

namespace opossum {

class NullOperation : public TuningOperation {
 public:
  virtual void execute() override;
  virtual void print_on(std::ostream& output) const override;
};

}  // namespace opossum
