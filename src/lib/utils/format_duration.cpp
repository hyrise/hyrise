#include "format_duration.hpp"

#include <sstream>

namespace opossum {

std::string format_duration(const std::chrono::nanoseconds nanoseconds) {
  auto nanosecond_count = nanoseconds.count();

  const auto microseconds = (nanosecond_count / 1'000) % 1000;
  const auto milliseconds = (nanosecond_count / 1'000'000) % 1000;
  const auto seconds = (nanosecond_count / 1'000'000'000) % 60;
  const auto minutes = (nanosecond_count / 60'000'000'000) % 1000;
  nanosecond_count %= 1000;

  std::stringstream stream;

  if (minutes > 0) {
    stream << minutes << "m " << seconds << "s";
  } else if (seconds > 0) {
    stream << seconds << "s " << milliseconds << " ms";
  } else if (milliseconds > 0) {
    stream << milliseconds << "ms " << microseconds << " µs";
  } else {
    stream << microseconds << "µs " << nanosecond_count << "ns";
  }

  return stream.str();
}

}  // namespace opossum
