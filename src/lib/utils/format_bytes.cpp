#include "format_bytes.hpp"

#include <iomanip>
#include <sstream>

namespace opossum {

std::string format_bytes(size_t bytes) {
  std::stringstream stream;

  const auto gigabytes = bytes / 1'000'000'000;
  const auto megabytes = (bytes / 1'000'000) % 1'000;
  const auto kilobytes = (bytes / 1'000) % 1'000;
  bytes %= 1000;

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
    stream << std::setfill('0') << std::setw(3) << bytes;
    stream << "KB";
  } else {
    stream << bytes << "B";
  }

  return stream.str();
}

}  // namespace opossum
