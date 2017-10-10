#pragma once

#include <string>
#include <type_traits>

namespace opossum {

unsigned int MurmurHash2(const void* key, int len, unsigned int seed);

// murmur hash for builtin number types (int, double)
template <typename T>
typename std::enable_if<std::is_arithmetic<T>::value, unsigned int>::type murmur2(T key, unsigned int seed) {
  return MurmurHash2(&key, sizeof(T), seed);
}

// murmur hash for std::string
template <typename T>
typename std::enable_if<std::is_same<T, std::string>::value, unsigned int>::type murmur2(T key, unsigned int seed) {
  return MurmurHash2(key.c_str(), key.size(), seed);
}

}  // namespace opossum
