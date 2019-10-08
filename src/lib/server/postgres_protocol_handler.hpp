#pragma once

#include "all_parameter_variant.hpp"
#include "network_message_types.hpp"
#include "ring_buffer.hpp"

namespace opossum {

// This struct stores information about a prepared statements required for execution.
struct PreparedStatementDetails {
  std::string statement_name;
  std::string portal;
  std::vector<AllTypeVariant> parameters;
};

// This class implements the message handling according to the PostgreSQL Wire Protocol.
template <typename SocketType>
class PostgresProtocolHandler {
 public:
  explicit PostgresProtocolHandler(const std::shared_ptr<SocketType>& socket);

  // Handling the startup packet
  uint32_t read_startup_packet();
  void read_startup_packet_body(const uint32_t size);

  // Read first byte of next packet
  NetworkMessageType read_packet_type();

  // Reading supported packets
  std::string read_query_packet();
  std::pair<std::string, std::string> read_parse_packet();
  void read_sync_packet();
  void read_describe_packet();
  PreparedStatementDetails read_bind_packet();
  std::string read_execute_packet();

  // Sending out appropiate packets
  void send_authentication();
  void send_parameter(const std::string& key, const std::string& value);
  void send_command_complete(const std::string& command_complete_message);
  void send_ready_for_query();
  void send_row_description(const std::string& column_name, const uint32_t object_id, const int16_t type_width);
  void send_data_row(const std::vector<std::string>& row_strings, const uint32_t string_lengths);

  // Send out status message containing NetworkMessageType and length
  void send_status_message(const NetworkMessageType message_type);

  // Send error message to client if there is an error during parsing or execution
  void send_error_message(const std::string& error_message);

  void send_execution_info(const std::string& execution_information);
  void set_row_description_header(const uint32_t total_column_name_length, const uint16_t column_count);

  // This method is required for testing. Otherwise we can't make the protocol handler flush its data.
  void force_flush() { _write_buffer.flush(); }

 private:
  void _ssl_deny();
  ReadBuffer<SocketType> _read_buffer;
  WriteBuffer<SocketType> _write_buffer;
};
}  // namespace opossum
