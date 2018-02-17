#include "postgres_wire_handler.hpp"

#include <iostream>
#include <iterator>

#include "sql/sql_pipeline.hpp"
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

  // Special SSL version number
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

PreparedStatementInfo PostgresWireHandler::handle_parse_packet(const InputPacket& packet, size_t length) {
  auto statement_name = read_string(packet);

  auto query = read_string(packet);

  auto n_parameter_data_types = read_value<uint16_t>(packet);

  /*auto parameter_data_types = */ read_values<uint32_t>(packet, n_parameter_data_types);

  //  SQLPipeline sql_pipeline{query};
  //  auto parsed_statements = sql_pipeline.get_parsed_sql_statements();
  //  if (parsed_statements.size() != 1) {
  //    throw std::runtime_error("Only exactly 1 statement supported.");
  //  }

  return PreparedStatementInfo{std::move(statement_name), std::move(query)};  //, std::move(sql_pipeline)};
}

std::vector<AllParameterVariant> PostgresWireHandler::handle_bind_packet(const InputPacket& packet, size_t length) {
  auto portal = read_string(packet);

  auto statement_name = read_string(packet);

  auto n_format_codes = read_value<int16_t>(packet);

  auto format_codes = read_values<int16_t>(packet, n_format_codes);

  auto n_parameter_values = ntohs(read_value<int16_t>(packet));

  std::vector<AllParameterVariant> parameter_values;
  for (auto i = 0; i < n_parameter_values; ++i) {
    auto parameter_value_length = ntohl(read_value<int32_t>(packet));
    auto x = read_values<char>(packet, parameter_value_length);
    const std::string x_str(x.begin(), x.end());
    parameter_values.emplace_back(std::stoi(x_str));
  }

  auto n_result_column_format_codes = read_value<int16_t>(packet);
  auto result_column_format_codes = read_values<int16_t>(packet, n_result_column_format_codes);

  return parameter_values;
}

std::string PostgresWireHandler::handle_execute_packet(const InputPacket& packet, size_t length) {
  const auto portal = read_string(packet);
  /*const auto max_rows = */ read_value<int32_t>(packet);
  return portal;
}

std::string PostgresWireHandler::handle_describe_packet(const InputPacket& packet, size_t length) {
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

std::string PostgresWireHandler::read_string(const InputPacket& packet) {
  std::string result(packet.data.data() + std::distance(packet.data.cbegin(), packet.offset));
  packet.offset += result.length() + 1;
  return result;
}

}  // namespace opossum
