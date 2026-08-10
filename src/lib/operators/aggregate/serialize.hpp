#pragma once

#include <cstddef>
#include <cstring>
#include <type_traits>
#include <vector>

namespace hyrise {

template <typename T>
  requires std::is_trivially_copyable_v<T>
void serialize_value(std::vector<std::byte>& buffer, const T& value) {
  const auto offset = buffer.size();
  buffer.resize(offset + sizeof(T));
  std::memcpy(buffer.data() + offset, &value, sizeof(T));
}

void serialize_value(std::vector<std::byte>& buffer, const pmr_string& value) {
  const auto offset = buffer.size();
  buffer.resize(offset + value.size());
  std::memcpy(buffer.data() + offset, value.data(), value.size());
}

template <typename T>
  requires std::is_trivially_copyable_v<T>
void serialize_value(std::vector<std::byte>& buffer, const T& value, bool is_null) {
  if (is_null) {
    buffer.push_back(std::byte{0x01});
    return;
  }
  const auto offset = buffer.size();
  buffer.resize(offset + 1 + sizeof(T));
  buffer[offset] = std::byte{0x00};
  std::memcpy(buffer.data() + offset + 1, &value, sizeof(T));
}

void serialize_value(std::vector<std::byte>& buffer, const pmr_string& value, bool is_null) {
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
