#pragma once

#include <string>
#include <type_traits>
#include <utility>

namespace opossum {

std::pair<uint64_t, uint64_t> murmur_hash3_x64_128(const void* key, const int len, const uint32_t seed);

// murmur hash for builtin number types
template <typename T>
typename std::enable_if<std::is_arithmetic<T>::value, std::pair<uint64_t, uint64_t>>::type murmur3(T key,
                                                                                                   uint32_t seed) {
  return murmur_hash3_x64_128(&key, sizeof(T), seed);
}

// murmur hash for pmr_string
template <typename T>
typename std::enable_if<std::is_same<T, pmr_string>::value, std::pair<uint64_t, uint64_t>>::type murmur3(
    T key, uint32_t seed) {
  return murmur_hash3_x64_128(key.c_str(), static_cast<int>(key.size()), seed);
}

}  // namespace opossum
