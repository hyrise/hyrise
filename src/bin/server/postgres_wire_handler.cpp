#include "postgres_wire_handler.hpp"

#include <iostream>

namespace {
uint32_t uint32_endian_swap(uint32_t num) {
  return ((num & 0xFF000000) >> 24) | ((num & 0x00FF0000) >> 8 ) | ((num & 0x0000FF00) << 8 ) | (num << 24);
}
}

namespace opossum {

uint32_t PostgresWireHandler::handle_startup_package(InputPacket& packet) {
  // Ignore length
  auto be_length = read_value<uint32_t>(packet);
  auto length = uint32_endian_swap(be_length);

  auto be_version = read_value<uint32_t>(packet);
  auto version = uint32_endian_swap(be_version);

  // Reset data buffer
  packet.offset = packet.data.begin();

  // Special SLL version number
  if (version == 80877103) {
    return 0;
  } else {
    return length - (2 * sizeof(uint32_t));
  }
}

void PostgresWireHandler::handle_startup_package_content(InputPacket& packet, size_t length) {
//  for (auto i = 0u; i < length; ++i) {
//    read_value<char>(packet);
//  }
  read_values<char>(packet, length);
}

std::string PostgresWireHandler::handle_query_packet(InputPacket& packet, size_t length) {
  auto buffer = read_values<char>(packet, length);

  packet.offset += length;
  return std::string(buffer.data(), buffer.size());
}

std::string PostgresWireHandler::read_string(InputPacket& packet) {
  auto str_length = read_value<uint32_t>(packet);
  auto buffer = read_values<char>(packet, str_length);
  packet.offset += str_length;
  return std::string(buffer.data(), buffer.size());
}

void PostgresWireHandler::write_string(OutputPacket& packet, const std::string& value) {
  auto num_bytes = value.length();
  auto& data = packet.data;
  data.reserve(data.size() + num_bytes + 1);

  data.insert(data.end(), value.begin(), value.end());
  // \0-terminate the string
  data.push_back(0);
}



}  // namespace opossum