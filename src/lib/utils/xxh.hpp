#pragma once

#include <string>
#include <type_traits>
#include <utility>

#include "xxhash.h"

namespace opossum {

// xxhash for builtin number types
template <typename T>
typename std::enable_if<std::is_arithmetic<T>::value, uint64_t>::type xx_hash(T key, uint32_t seed) {
  return XXH64(&key, sizeof(T), seed);
}

// xxhash for pmr_string
template <typename T>
typename std::enable_if<std::is_same<T, pmr_string>::value, uint64_t>::type xx_hash(T key, uint32_t seed) {
  return XXH64(key.c_str(), static_cast<int>(key.size()), seed);
}

}  // namespace oposs