#pragma once

#include <chrono>

namespace opossum {

struct BaseOperatorPerformanceData {
  virtual ~BaseOperatorPerformanceData() = default;

  std::chrono::microseconds total{0};
};

}  // namespace opossum
