#pragma once

#include <cstddef>
#include <cstring>
#include <type_traits>
#include <vector>

namespace hyrise {

// Estimate the serialized value size of values for the given type. This overestimates in case of many null values.
template <typename T>
  requires std::is_trivially_copyable_v<T>
constexpr size_t serialized_value_size(const bool is_nullable) {
  return is_nullable ? 1 + sizeof(T) : sizeof(T);
}

// This is just a wild guess.
static constexpr auto ESTIMATED_AVERAGE_STRING_LENGTH = size_t{3};

// Estimate the serialized value size of string values. This overestimates in case of many null values.
template <typename T>
  requires std::is_same_v<T, pmr_string>
constexpr size_t serialized_value_size(const bool is_nullable) {
  return is_nullable ? 1 + ESTIMATED_AVERAGE_STRING_LENGTH : ESTIMATED_AVERAGE_STRING_LENGTH;
}

// Write the byte representation of the value into the byte buffer.
template <typename T>
  requires std::is_trivially_copyable_v<T>
void serialize_value(std::vector<std::byte>& buffer, const T& value) {
  const auto offset = buffer.size();
  buffer.resize(offset + sizeof(T));
  std::memcpy(buffer.data() + offset, &value, sizeof(T));
}

// Write the byte representation of the string into the byte buffer.
inline void serialize_value(std::vector<std::byte>& buffer, const pmr_string& value) {
  const auto offset = buffer.size();
  buffer.resize(offset + value.size());
  std::memcpy(buffer.data() + offset, value.data(), value.size());
}

// If `is_null` is true, write a single 0x00 byte into the buffer. If `is_null` is false
// write a single 0x01 byte into the buffer followed by the byte representation of the value.
template <typename T>
  requires std::is_trivially_copyable_v<T>
void serialize_value(std::vector<std::byte>& buffer, const T& value, const bool is_null) {
  if (is_null) {
    buffer.push_back(std::byte{0x01});
    return;
  }
  const auto offset = buffer.size();
  buffer.resize(offset + 1 + sizeof(T));
  buffer[offset] = std::byte{0x00};
  std::memcpy(buffer.data() + offset + 1, &value, sizeof(T));
}

// If `is_null` is true, write a single 0x00 byte into the buffer. If `is_null` is false
// write a single 0x01 byte into the buffer followed by the byte representation of the string.
inline void serialize_value(std::vector<std::byte>& buffer, const pmr_string& value, const bool is_null) {
  if (is_null) {
    buffer.push_back(std::byte{0x01});
    return;
  }
  const auto offset = buffer.size();
  buffer.resize(offset + 1 + value.size());
  buffer[offset] = std::byte{0x00};
  std::memcpy(buffer.data() + offset + 1, value.data(), value.size());
}

}  // namespace hyrise
