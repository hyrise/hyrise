#pragma once

#include <algorithm>
#include <string>
#include <vector>


namespace opossum {

using ByteBuffer = std::vector<char>;

struct InputPacket {
  ByteBuffer data = ByteBuffer(1024);
  ByteBuffer::iterator offset = data.begin();
  ByteBuffer::iterator end;
};

struct OutputPacket {
  ByteBuffer data;
};


class PostgresWireHandler {
 public:
  static uint32_t handle_startup_package(InputPacket& packet);
  static void handle_startup_package_content(InputPacket& packet, size_t length);
  static std::string handle_query_packet(InputPacket& packet, size_t length);

  template <typename T>
  static T read_value(InputPacket& packet);

  template <typename T>
  static std::vector<T> read_values(InputPacket& packet, size_t num_values);

  static std::string read_string(InputPacket& packet);

  static uint32_t get_uint32_big_endian(uint32_t num);

  template <typename T>
  static void write_value(OutputPacket& packet, T value);

  static void write_string(OutputPacket& packet, const std::string& value);
};

template <typename T>
T PostgresWireHandler::read_value(InputPacket& packet) {
  T result;
  auto num_bytes = sizeof(T);
  // TODO: bounds check
  std::copy(packet.offset, packet.offset + num_bytes, reinterpret_cast<char*>(&result));
  packet.offset += num_bytes;
  return result;
}

template <typename T>
std::vector<T> PostgresWireHandler::read_values(InputPacket& packet, const size_t num_values) {
  std::vector<T> result;
  result.reserve(num_values);
  auto num_bytes = num_values * sizeof(T);
  // TODO: bounds check
  std::copy(packet.offset, packet.offset + num_bytes, reinterpret_cast<char*>(result.data()));
  packet.offset += num_bytes;
  return result;
}

template <typename T>
void PostgresWireHandler::write_value(OutputPacket& packet, T value) {
  auto num_bytes = sizeof(T);
  auto value_chars = reinterpret_cast<char*>(&value);

  auto& data = packet.data;
  data.reserve(data.size() + num_bytes);

  for (auto byte_offset = 0u; byte_offset < num_bytes; ++byte_offset) {
    data.push_back(value_chars[byte_offset]);
  }
}


}  // namespace opossum