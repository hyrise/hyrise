#include "format_duration.hpp"

#include <sstream>

namespace opossum {

std::string format_duration(uint64_t total_nanoseconds) {
  uint64_t nanoseconds_remaining = total_nanoseconds;

  const auto minutes = nanoseconds_remaining / 60'000'000'000;
  nanoseconds_remaining -= minutes * 60'000'000'000;

  const auto seconds = nanoseconds_remaining / 1'000'000'000;
  nanoseconds_remaining -= seconds * 1'000'000'000;

  const auto milliseconds = nanoseconds_remaining / 1'000'000;
  nanoseconds_remaining -= milliseconds * 1'000'000;

  const auto microseconds = nanoseconds_remaining / 1'000;
  nanoseconds_remaining -= microseconds * 1'000;

  const auto nanoseconds = nanoseconds_remaining;

  std::stringstream stream;

  if (minutes > 0) {
    stream << minutes << " min " << seconds << " s";
  } else if (seconds > 0) {
    stream << seconds << " s " << milliseconds << " ms";
  } else if (milliseconds > 0) {
    stream << milliseconds << " ms " << microseconds << " µs";
  } else if (microseconds > 0) {
    stream << microseconds << " µs " << nanoseconds << " ns";
  } else {
    stream << nanoseconds << " ns";
  }

  return stream.str();
}

}  // namespace opossum
