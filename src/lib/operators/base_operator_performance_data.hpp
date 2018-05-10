#pragma once

#include <chrono>

namespace opossum {

struct BaseOperatorPerformanceData final {
  std::chrono::microseconds walltime{0};
};

}  // namespace opossum
