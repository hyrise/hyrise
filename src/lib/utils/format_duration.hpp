#pragma once

#include <chrono> // NEEDEDINCLUDE
#include <string> // NEEDEDINCLUDE

namespace opossum {

// "3h 42min" instead of "2,53×10¹²ns"
std::string format_duration(const std::chrono::nanoseconds& total_nanoseconds);

}  // namespace opossum
