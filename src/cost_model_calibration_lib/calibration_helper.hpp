#pragma once

#include <chrono>
#include <string>

namespace opossum {

template <typename T>
std::string get_time_as_iso_string(std::chrono::time_point<T> now) {
  auto timestamp = std::chrono::system_clock::to_time_t(now);

  std::ostringstream ss;
  ss << std::put_time(std::localtime(&timestamp), "%FT%H-%M-%S");
  
  return ss.str();
}

}  // namespace opossum
