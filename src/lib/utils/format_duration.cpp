#include "format_duration.hpp"

#include <sstream>

namespace opossum {

std::string format_duration(uint64_t nanoseconds) {
  const auto microseconds = (nanoseconds / 1'000) % 1000;
  const auto milliseconds = (nanoseconds / 1'000'000) % 1000;
  const auto seconds = (nanoseconds / 1'000'000'000) % 1000;
  const auto minutes = (nanoseconds / 60'000'000'000) % 1000;
  nanoseconds %= 1000;

  std::stringstream stream;

  if (minutes > 0) {
    stream << minutes << "m " << seconds << "s";
  } else if (seconds > 0) {
    stream << seconds << "s " << milliseconds << " ms";
  } else if (milliseconds > 0) {
    stream << milliseconds << "ms " << microseconds << " µs";
  } else {
    stream << microseconds << "µs " << nanoseconds << "ns";
  }

  return stream.str();
}

}  // namespace opossum
