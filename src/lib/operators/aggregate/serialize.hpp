#pragma once

namespace hyrise {

template <typename T>
  requires std::is_trivially_copyable_v<T>
void serialize_value(std::vector<std::byte>& buffer, T value) {
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
void serialize_value(std::vector<std::byte>& buffer, T value, bool is_null) {
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

template <typename T, bool Nullable>
  requires std::is_trivially_copyable_v<T> && (!Nullable)
T deserialize_value(const std::span<const std::byte>& bytes) {
  T value;
  std::memcpy(&value, bytes.data(), sizeof(T));
  return value;
}

template <typename T, bool Nullable>
  requires std::is_trivially_copyable_v<T> && Nullable
std::optional<T> deserialize_value(const std::span<const std::byte>& bytes) {
  if (bytes[0] == std::byte{0x01}) {
    return std::nullopt;
  }
  T value;
  std::memcpy(&value, bytes.data() + 1, sizeof(T));
  return value;
}

template <typename T, bool Nullable>
  requires std::is_same_v<T, pmr_string> && (!Nullable)
pmr_string deserialize_value(const std::span<const std::byte>& bytes) {
  return pmr_string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
}

template <typename T, bool Nullable>
  requires std::is_same_v<T, pmr_string> && Nullable
std::optional<pmr_string> deserialize_value(const std::span<const std::byte>& bytes) {
  if (bytes[0] == std::byte{0x01}) {
    return std::nullopt;
  }
  return pmr_string(reinterpret_cast<const char*>(bytes.data() + 1), bytes.size() - 1);
}

}  // namespace hyrise
