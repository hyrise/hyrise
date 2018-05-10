#pragma once

#include <chrono>

namespace opossum {

/**
 * Starts a std::chrono::high_resolution_clock base timer on construction and returns and resets measurement when
 * lap() is called.
 */
class Timer final {
 public:
  Timer();

  std::chrono::microseconds lap();

 private:
  std::chrono::high_resolution_clock::time_point _begin;
};

}  // namespace opossum
