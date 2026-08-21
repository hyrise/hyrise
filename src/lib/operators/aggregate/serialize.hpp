#pragma once

#include <cstddef>
#include <cstring>
#include <type_traits>
#include <vector>

namespace hyrise {

// Write the byte representation of the value into the byte buffer.
template <typename T>
  requires std::is_trivially_copyable_v<T>
void serialize_value(std::vector<std::byte>& buffer, const T& value) {
  const auto offset = buffer.size();
  buffer.resize(offset + sizeof(T));
  std::memcpy(buffer.data() + offset, &value, sizeof(T));
}

// Write the byte representation of the string into the byte buffer.
void serialize_value(std::vector<std::byte>& buffer, const pmr_string& value) {
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
void serialize_value(std::vector<std::byte>& buffer, const pmr_string& value, const bool is_null) {
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
