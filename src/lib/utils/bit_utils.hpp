#pragma once

#include <type_traits>

namespace opossum {

template<typename T>
bool is_power_of_two(const T& value) {
  // Check whether n is power of two: https://stackoverflow.com/a/108360
  static_assert(std::is_integral_v<T>);
  return (value & (value - 1)) == 0 && value > 0;
}

template<typename T> std::enable_if_t<sizeof(T) == 8, T> ceil_power_of_two(T value) {
  // Taken from https://stackoverflow.com/a/466242
  --value;
  value |= value >> 1;
  value |= value >> 2;
  value |= value >> 4;
  value |= value >> 8;
  value |= value >> 16;
  value |= value >> 32;
  ++value;
  return value;
}

}  // namespace opossum