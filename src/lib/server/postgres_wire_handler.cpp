#include "postgres_wire_handler.hpp"

#include <iostream>
#include <iterator>

#include "sql/sql_pipeline.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

uint32_t PostgresWireHandler::handle_startup_package(const InputPacket& packet) {
  auto network_length = read_value<uint32_t>(packet);
  // We ALWAYS need to convert from network endianess to host endianess with these fancy macros
  // ntohl = network to host long and htonl = host to network long (where long = uint32)
  // We use the prefix network_ to indicate that the value still needs to be converted.
  // TODO(lawben): This should be integrated into the read_value template for numeric data types at some point
  auto length = ntohl(network_length);

  auto network_version = read_value<uint32_t>(packet);
  auto version = ntohl(network_version);

  // Reset data buffer
  packet.offset = packet.data.cbegin();

  // Special SSL version number that we catch to deny SSL support
  if (version == 80877103) {
    return 0;
  } else {
    // Subtract read bytes from total length
    return length - (2 * sizeof(uint32_t));
  }
}

void PostgresWireHandler::handle_startup_package_content(const InputPacket& packet) {
  // Ignore the content, because we don't care about the variables that were passed in.
  read_values<char>(packet, packet.data.size());
}

RequestHeader PostgresWireHandler::handle_header(const InputPacket& packet) {
  auto tag = read_value<NetworkMessageType>(packet);

  auto network_length = read_value<uint32_t>(packet);
  auto length = ntohl(network_length);

  packet.offset = packet.data.begin();

  // Return length minus the already read bytes (the message type doesn't count into the length)
  return {/* message_type = */ tag, /* payload_length = */ static_cast<uint32_t>(length - sizeof(network_length))};
}

std::string PostgresWireHandler::handle_query_packet(const InputPacket& packet) { return read_string(packet); }

ParsePacket PostgresWireHandler::handle_parse_packet(const InputPacket& packet) {
  auto statement_name = read_string(packet);

  auto query = read_string(packet);

  auto network_parameter_data_types = read_value<uint16_t>(packet);

  /*auto parameter_data_types = */ read_values<uint32_t>(packet, network_parameter_data_types);

  return ParsePacket{std::move(statement_name), std::move(query)};
}

BindPacket PostgresWireHandler::handle_bind_packet(const InputPacket& packet) {
  auto portal = read_string(packet);

  auto statement_name = read_string(packet);

  auto num_format_codes = ntohs(read_value<int16_t>(packet));

  auto format_codes = read_values<int16_t>(packet, num_format_codes);

  auto num_parameter_values = ntohs(read_value<int16_t>(packet));

  std::vector<AllTypeVariant> parameter_values;
  for (auto i = 0; i < num_parameter_values; ++i) {
    auto parameter_value_length = ntohl(read_value<int32_t>(packet));
    auto x = read_values<char>(packet, parameter_value_length);
    const std::string x_str(x.begin(), x.end());
    parameter_values.emplace_back(x_str);
  }

  auto num_result_column_format_codes = ntohs(read_value<int16_t>(packet));
  auto result_column_format_codes = read_values<int16_t>(packet, num_result_column_format_codes);

  return BindPacket{statement_name, portal, std::move(parameter_values)};
}

std::string PostgresWireHandler::handle_execute_packet(const InputPacket& packet) {
  const auto portal = read_string(packet);
  /*const auto max_rows = */ read_value<int32_t>(packet);
  return portal;
}

std::string PostgresWireHandler::handle_describe_packet(const InputPacket& packet) {
  read_value<char>(packet);
  const auto portal = read_string(packet);
  return portal;
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

std::shared_ptr<OutputPacket> PostgresWireHandler::new_output_packet(NetworkMessageType type) {
  auto output_packet = std::make_shared<OutputPacket>();
  write_value(*output_packet, type);
  write_value(*output_packet, htonl(0u));
  return output_packet;
}

void PostgresWireHandler::write_output_packet_size(OutputPacket& packet) {
  auto& data = packet.data;
  Assert(
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

std::string PostgresWireHandler::read_string(const InputPacket& packet) {
  if (packet.offset == packet.data.cend()) return "";

  // We have to convert the byte buffer into a std::string, making sure
  // we don't run past the end of the buffer and stop at the first null byte
  auto string_end = std::find(packet.offset, packet.data.cend(), '\0');
  std::string result(packet.offset, string_end);

  packet.offset += result.length() + 1;

  return result;
}

}  // namespace opossum
