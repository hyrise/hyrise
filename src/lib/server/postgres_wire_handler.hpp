#pragma once

#include <arpa/inet.h>
#include <algorithm>
#include <string>
#include <vector>

#include "types.hpp"

namespace opossum {

// For convenience
using ByteBuffer = std::vector<char>;

static const uint32_t MAX_BUFFER_SIZE = 1024u;

// This is the struct that we store our incoming network bytes in and that we then read from
struct InputPacket {
  // The vector needs to have a fixed size for boost::asio to work with it
  ByteBuffer data = ByteBuffer(MAX_BUFFER_SIZE);

  // Stores the current position in the data buffer
  mutable ByteBuffer::const_iterator offset = data.begin();
};

// This is the struct in which we write our network bytes and then send
struct OutputPacket {
  ByteBuffer data;
};

struct RequestHeader {
  NetworkMessageType message_type;
  uint32_t payload_length;
};

class PostgresWireHandler {
 public:
  static OutputPacket new_output_packet(NetworkMessageType type);
  static void write_output_packet_size(OutputPacket& packet);

  static uint32_t handle_startup_package(const InputPacket& packet);
  static void handle_startup_package_content(const InputPacket& packet, size_t length);

  static RequestHeader handle_header(const InputPacket& packet);

  static std::string handle_query_packet(const InputPacket& packet, size_t length);

  template <typename T>
  static T read_value(const InputPacket& packet);

  template <typename T>
  static std::vector<T> read_values(const InputPacket& packet, size_t num_values);

  template <typename T>
  static void write_value(OutputPacket& packet, T value);

  static void write_string(OutputPacket& packet, const std::string& value, bool terminate = true);
};

template <typename T>
T PostgresWireHandler::read_value(const InputPacket& packet) {
  T result;
  auto num_bytes = sizeof(T);
  // TODO(lawben): bounds check
  std::copy(packet.offset, packet.offset + num_bytes, reinterpret_cast<char*>(&result));
  packet.offset += num_bytes;
  return result;
}

template <typename T>
std::vector<T> PostgresWireHandler::read_values(const InputPacket& packet, const size_t num_values) {
  std::vector<T> result(num_values);
  auto num_bytes = result.size() * sizeof(T);

  // TODO(lawben): bounds check

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
