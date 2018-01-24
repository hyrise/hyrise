#pragma once

#include <string>
#include <type_traits>

namespace opossum {

unsigned int murmur_hash2(const void* key, int len, unsigned int seed);

// murmur hash for builtin number types (int, double)
template <typename T>
typename std::enable_if<std::is_arithmetic<T>::value, unsigned int>::type murmur2(T key, unsigned int seed) {
  /* 
   * Convert all numerics to the highest precision type (double).
   * This will cause the numbers 5 and 5.0 to have the same hash value.
   */
  auto double_value = static_cast<double>(key);
  return murmur_hash2(&double_value, sizeof(double), seed);
}

// murmur hash for std::string
template <typename T>
typename std::enable_if<std::is_same<T, std::string>::value, unsigned int>::type murmur2(T key, unsigned int seed) {
  return murmur_hash2(key.c_str(), key.size(), seed);
}

}  // namespace opossum
