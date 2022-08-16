#include "timer.hpp"

#include "utils/format_duration.hpp"

namespace hyrise {

Timer::Timer() {
  _begin = std::chrono::steady_clock::now();
}

std::chrono::nanoseconds Timer::lap() {
  const auto now = std::chrono::steady_clock::now();
  const auto lap_duration = std::chrono::nanoseconds{now - _begin};
  _begin = now;
  return lap_duration;
}

std::string Timer::lap_formatted() {
  return format_duration(lap());
}

}  // namespace hyrise
