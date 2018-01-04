#include "postgres_wire_handler.hpp"

#include <iostream>

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

uint32_t PostgresWireHandler::handle_startup_package(const InputPacket& packet) {
  auto n_length = read_value<uint32_t>(packet);
  // We ALWAYS need to convert from network endianess to host endianess with these fancy macros
  // ntohl = network to host long and htonl = host to network long (where long = uint32)
  // TODO(lawben): This should be integrated into the read_value template for numeric data types at some point
  auto length = ntohl(n_length);

  auto n_version = read_value<uint32_t>(packet);
  auto version = ntohl(n_version);

  // Reset data buffer
  packet.offset = packet.data.cbegin();

  // Special SLL version number
  if (version == 80877103) {
    return 0;
  } else {
    // Subtract read bytes from total length
    return length - (2 * sizeof(uint32_t));
  }
}

void PostgresWireHandler::handle_startup_package_content(const InputPacket& packet, size_t length) {
  // Ignore the content for now
  read_values<char>(packet, length);
}

RequestHeader PostgresWireHandler::handle_header(const InputPacket& packet) {
  auto tag = read_value<NetworkMessageType>(packet);
  std::cout << "Received message tag: " << static_cast<unsigned char>(tag) << std::endl;

  auto n_length = read_value<uint32_t>(packet);
  auto length = ntohl(n_length);

  packet.offset = packet.data.begin();

  // Return length minus the already read bytes (the message type doesn't count into the length)
  return {/* message_type = */ tag, /* payload_length = */ static_cast<uint32_t>(length - sizeof(n_length))};
}

std::string PostgresWireHandler::handle_query_packet(const InputPacket& packet, size_t length) {
  auto buffer = read_values<char>(packet, length);

  // Convert the content to a string for now
  return std::string(buffer.data(), buffer.size());
}

void PostgresWireHandler::write_string(OutputPacket& packet, const std::string& value, bool terminate) {
  auto num_bytes = value.length();
  auto& data = packet.data;

  // Add one byte more for terminated string
  auto total_size = data.size() + num_bytes + (terminate ? 1 : 0);
  data.reserve(total_size);

  data.insert(data.end(), value.cbegin(), value.cend());

  if (terminate) {
    // 0-terminate the string
    data.emplace_back('\0');
  }
}

OutputPacket PostgresWireHandler::new_output_packet(NetworkMessageType type) {
  OutputPacket output_packet;
  write_value(output_packet, type);
  write_value(output_packet, htonl(0u));
  return output_packet;
}

void PostgresWireHandler::write_output_packet_size(OutputPacket& packet) {
  auto& data = packet.data;
  DebugAssert(
      data.size() >= 5,
      "Cannot update the packet size of a packet which is less than NetworkIMessageType + dummy size (i.e. 5 bytes)");

  // - 1 because the message type byte does not contribute to the total size
  auto total_bytes = htonl(data.size() - 1);
  auto size_chars = reinterpret_cast<char*>(&total_bytes);

  // The size starts at byte position 1
  const auto size_offset = 1u;
  for (auto byte_offset = 0u; byte_offset < sizeof(total_bytes); ++byte_offset) {
    data[size_offset + byte_offset] = size_chars[byte_offset];
  }
}

}  // namespace opossum
