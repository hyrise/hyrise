#include "memory_usage.hpp"

#include <iomanip>

namespace opossum {

MemoryUsage::MemoryUsage(const size_t bytes) : bytes(bytes) {}

void MemoryUsage::print(std::ostream& stream) const {
  const auto gigabytes = bytes / 1'000'000'000;
  const auto megabytes = (bytes / 1'000'000) % 1'000;
  const auto kilobytes = (bytes / 1'000) % 1'000;

  const auto previous_fill = stream.fill();
  if (gigabytes > 0) {
    stream << gigabytes << ".";
    stream << std::setfill('0') << std::setw(3) << megabytes;
    stream << "GB";
  } else if (megabytes > 0) {
    stream << megabytes << ".";
    stream << std::setfill('0') << std::setw(3) << kilobytes;
    stream << "MB";
  } else if (kilobytes > 0) {
    stream << kilobytes << ".";
    stream << std::setfill('0') << std::setw(3) << kilobytes;
    stream << "KB";
  } else {
    stream << bytes << "B";
  }

  stream << std::setfill(previous_fill);
}

}  // namespace opossum
