#include "format_duration.hpp"

#include <cmath>
#include <sstream>

#include "utils/assert.hpp"

namespace opossum {

using namespace std::chrono_literals;  // NOLINT

std::string format_duration(const std::chrono::nanoseconds& total_nanoseconds) {
  std::stringstream stream;
  const auto minutes = std::chrono::floor<std::chrono::minutes>(total_nanoseconds);
  if (minutes > 0min) {
    stream << minutes.count() << " min "
           << std::chrono::round<std::chrono::seconds>(total_nanoseconds - minutes).count() << " s";
    return stream.str();
  }

  const auto seconds = std::chrono::floor<std::chrono::seconds>(total_nanoseconds);
  if (seconds > 0s) {
    stream << seconds.count() << " s "
           << std::chrono::round<std::chrono::milliseconds>(total_nanoseconds - seconds).count() << " ms";
    return stream.str();
  }

  const auto milliseconds = std::chrono::floor<std::chrono::milliseconds>(total_nanoseconds);
  if (milliseconds > 0ms) {
    stream << milliseconds.count() << " ms "
           << std::chrono::round<std::chrono::microseconds>(total_nanoseconds - milliseconds).count() << " µs";
    return stream.str();
  }

  const auto microseconds = std::chrono::floor<std::chrono::microseconds>(nanoseconds_remaining);
  if (microseconds > 0us) {
    stream << microseconds.count() << " µs " << std::chrono::nanoseconds{total_nanoseconds - milliseconds}.count()
           << " ns";
    return stream.str();
  }

  stream << total_nanoseconds.count() << " ns";
  return stream.str();
}

}  // namespace opossum
