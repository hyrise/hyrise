#include "timer.hpp"

namespace opossum {

Timer::Timer() { _begin = std::chrono::high_resolution_clock::now(); }

std::chrono::microseconds Timer::lap() {
  const auto now = std::chrono::high_resolution_clock::now();
  const auto lap_ns = std::chrono::duration_cast<std::chrono::microseconds>(now - _begin);
  _begin = now;
  return lap_ns;
}

}  // namespace opossum
