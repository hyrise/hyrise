#include "format_duration.hpp"

#include <cmath>
#include <sstream>

#include "utils/assert.hpp"

namespace opossum {

std::string format_duration(const std::chrono::nanoseconds& total_nanoseconds) {
  // Returns the result of a positive integer division rounded both to zero and to the nearest int so that 60900ms
  // are returned as 61s, not 60s
  const auto div_both = [](const auto a, const auto b) {
    DebugAssert(a >= 0 && b > 0, "Can only operate on positive integers");
    const auto round_to_zero = a / b;
    auto round_to_nearest = a / b;
    if (a - round_to_zero * b >= b / 2) round_to_nearest += 1;
    return std::make_pair(round_to_zero, round_to_nearest);
  };

  auto nanoseconds_remaining = total_nanoseconds.count();

  const auto minutes = div_both(nanoseconds_remaining, 60'000'000'000);
  nanoseconds_remaining -= minutes.first * 60'000'000'000;

  const auto seconds = div_both(nanoseconds_remaining, 1'000'000'000);
  nanoseconds_remaining -= seconds.first * 1'000'000'000;

  const auto milliseconds = div_both(nanoseconds_remaining, 1'000'000);
  nanoseconds_remaining -= milliseconds.first * 1'000'000;

  const auto microseconds = div_both(nanoseconds_remaining, 1'000);
  nanoseconds_remaining -= microseconds.first * 1'000;

  const auto nanoseconds = nanoseconds_remaining;

  std::stringstream stream;

  if (minutes.first > 0) {
    stream << minutes.first << " min " << seconds.second << " s";
  } else if (seconds.first > 0) {
    stream << seconds.first << " s " << milliseconds.second << " ms";
  } else if (milliseconds.first > 0) {
    stream << milliseconds.first << " ms " << microseconds.second << " µs";
  } else if (microseconds.first > 0) {
    stream << microseconds.first << " µs " << nanoseconds << " ns";
  } else {
    stream << nanoseconds << " ns";
  }

  return stream.str();
}

}  // namespace opossum
