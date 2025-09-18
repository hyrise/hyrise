#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

#include "utils/assert.hpp"

using StringSize = unsigned char;
constexpr size_t STRING_SIZE_LENGTH = sizeof(StringSize);

namespace hyrise {

/*
 * Write an unsigned integer value in big-endian order (MSB first) into dest[1..length],
 * for lexicographic ordering etc.
 */
template <typename UInt>
inline void write_big_endian(std::byte* dest, UInt value, size_t length) {
  for (size_t byte_idx = 0; byte_idx < length; ++byte_idx) {
    dest[1 + byte_idx] = static_cast<std::byte>((value >> ((length - 1 - byte_idx) * 8)) & static_cast<UInt>(0xFF));
  }
}

inline void encode_string(std::byte* dest, const size_t data_length, const pmr_string& value) {
  const auto string_len = value.size();
  DebugAssert(string_len <= data_length - STRING_SIZE_LENGTH, "String length exceeds allocated buffer.");
  memset(dest, 0, data_length);  // Set all bytes to 0.
  // Copy the string data into the key buffer.
  memcpy(dest, value.data(), string_len);  //NOLINT
  // Store actual string length. This is required for cases, where the string ends in
  // null byte(s), because they cannot be differentiated from padding.
  memset(dest + data_length - STRING_SIZE_LENGTH, static_cast<StringSize>(string_len), STRING_SIZE_LENGTH);
}

inline void encode_double(std::byte* dest, const double value) {
  // Encode double value; reinterpret double as raw 64-bit bits.
  auto bits = std::bit_cast<uint64_t>(value);

  // Flip the bits to ensure lexicographic order matches numeric order.
  if (std::signbit(value)) {
    bits = ~bits;  // Negative values are bitwise inverted.
  } else {
    bits ^= 0x8000000000000000ULL;  // Flip the sign bit for positive values.
  }

  write_big_endian(dest, bits, 8);
}

inline void encode_float(std::byte* dest, const float value) {
  auto bits = std::bit_cast<uint32_t>(value);

  // Flip the bits to ensure lexicographic order matches numeric order.
  if (std::signbit(value)) {
    bits = ~bits;  // Negative values are bitwise inverted.
  } else {
    bits ^= 0x80000000;  // Flip the sign bit for positive values.
  }

  write_big_endian(dest, bits, 4);
}

template <typename T>
inline void encode_integer(std::byte* dest, const T value, const size_t data_length) {
  // Bias the value to get a lexicographically sortable encoding.
  using UnsignedT = std::make_unsigned_t<T>;
  const auto biased =
      static_cast<UnsignedT>(value) ^ (static_cast<UnsignedT>(1) << ((data_length * 8) - 1));  // Flip sign bit.

  write_big_endian(dest, biased, data_length);
}

}  // namespace hyrise
