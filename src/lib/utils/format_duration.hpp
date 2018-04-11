#pragma once

#include <chrono>
#include <cstdint>
#include <string>

namespace opossum {

// "3h 42min" instead of "2,53×10¹²ns"
std::string format_duration(const std::chrono::nanoseconds nanoseconds);

}  // namespace opossum
