#include "postgres_wire_handler.hpp"

#include <iostream>

//namespace {
//uint32_t uint32_endian_swap(uint32_t num) {
//  return ((num & 0xFF000000) >> 24) | ((num & 0x00FF0000) >> 8 ) | ((num & 0x0000FF00) << 8 ) | (num << 24);
//}
//}

namespace opossum {

uint32_t PostgresWireHandler::handle_startup_package(InputPacket& packet) {
  auto n_length = read_value<uint32_t>(packet);
  // We ALWAYS need to convert from network endianess to host endianess with these fancy macros
  // ntohl = network to host long and htonl = host to network long (where long = uint32)
  // TODO: This should be integrated into the read_value template for numeric data types at some point
  auto length = ntohl(n_length);

  auto n_version = read_value<uint32_t>(packet);
  auto version = ntohl(n_version);

  // Reset data buffer
  packet.offset = packet.data.begin();

  // Special SLL version number
  if (version == 80877103) {
    return 0;
  } else {
    // Subtract read bytes from total length
    return length - (2 * sizeof(uint32_t));
  }
}

void PostgresWireHandler::handle_startup_package_content(InputPacket& packet, size_t length) {
  // Ignore the content for now
  read_values<char>(packet, length);
}

uint32_t PostgresWireHandler::handle_header(InputPacket& packet) {
  auto tag = read_value<char>(packet);
  std::cout << "Received message tag: " << tag << std::endl;

  auto n_length = read_value<uint32_t>(packet);
  auto length = ntohl(n_length);

  packet.offset = packet.data.begin();

  // Return length minus the already read bytes
  return length - (sizeof(char) + sizeof(uint32_t));
}

std::string PostgresWireHandler::handle_query_packet(InputPacket& packet, size_t length) {
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

  data.insert(data.end(), value.begin(), value.end());

  if (terminate) {
    // 0-terminate the string
    data.push_back(0);
  }
}

}  // namespace opossum