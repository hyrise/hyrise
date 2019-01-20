#include "timer.hpp"

#include "utils/format_duration.hpp"

namespace opossum {

Timer::Timer() { _begin = std::chrono::high_resolution_clock::now(); }

std::chrono::nanoseconds Timer::lap() {
  const auto now = std::chrono::high_resolution_clock::now();
  const auto lap_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(now - _begin);
  _begin = now;
  return lap_duration;
}

std::string Timer::lap_formatted() { return format_duration(lap()); }

}  // namespace opossum
