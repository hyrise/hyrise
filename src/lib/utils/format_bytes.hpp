#pragma once

#include <cstdint>
#include <string>

namespace hyrise {

/**
 * @returns a string with 'bytes' formatted using a unit such as "3.405KB" or "360.420GB"
 */
std::string format_bytes(size_t bytes);

}  // namespace hyrise
