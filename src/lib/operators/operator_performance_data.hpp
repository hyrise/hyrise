#pragma once

#include <chrono>
#include <string>

#include "types.hpp"

namespace opossum {

// For an example on how this can be extended on a per-operator basis, see JoinIndex

struct OperatorPerformanceData : public Noncopyable {
  virtual ~OperatorPerformanceData() = default;

  std::chrono::nanoseconds walltime{0};

  virtual std::string to_string(DescriptionMode description_mode = DescriptionMode::SingleLine) const;
};

}  // namespace opossum
