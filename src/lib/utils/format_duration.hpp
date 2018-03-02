#pragma once

#include <cstdint>
#include <string>

namespace opossum {

// "3h 42min" instead of "2,53×10¹²ns"
std::string format_duration(uint64_t nanoseconds);

}  // namespace opossum
