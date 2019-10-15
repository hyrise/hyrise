#pragma once

#include "all_type_variant.hpp"
#include "postgres_message_types.hpp"
#include "ring_buffer.hpp"

namespace opossum {

// This struct stores information about a prepared statement.
struct PreparedStatementDetails {
  std::string statement_name;
  std::string portal;
  std::vector<AllTypeVariant> parameters;
};

// This class extracts information from client messages and serializes the response data according to the PostgreSQL
// Wire Protocol.
template <typename SocketType>
class PostgresProtocolHandler {
 public:
  explicit PostgresProtocolHandler(const std::shared_ptr<SocketType>& socket);

  // Handling the startup packet header returning the body's size
  uint32_t read_startup_packet_header();
  void read_startup_packet_body(const uint32_t size);

  // Setup new connection: successful authentication + sending parameters
  void send_authentication_response();
  void send_parameter(const std::string& key, const std::string& value);

  // Ready to receive a new packet
  void send_ready_for_query();

  // Read first byte of next packet to determine its type
  PostgresMessageType read_packet_type();

  // Reading SQL query packet
  std::string read_query_packet();

  // Send query result
  void send_row_description_header(const uint32_t total_column_name_length, const uint16_t column_count);
  void send_row_description(const std::string& column_name, const uint32_t object_id, const int16_t type_width);
  void send_values_as_strings(const std::vector<std::optional<std::string>>& row_strings,
                              const uint32_t string_lengths);
  void send_command_complete(const std::string& command_complete_message);

  // Series of packets for handling prepared statements
  std::pair<std::string, std::string> read_parse_packet();
  void read_sync_packet();

  // Send out status message containing PostgresMessageType and length
  void send_status_message(const PostgresMessageType message_type);
  void read_describe_packet();
  PreparedStatementDetails read_bind_packet();
  std::string read_execute_packet();

  // Send error message to client if there is an error during parsing or execution
  void send_error_message(const std::string& error_message);

  // Additional (optional) message containing execution times of different components (such as translator or optimizer)
  void send_execution_info(const std::string& execution_information);

  // This method is required for testing. Otherwise we can't make the protocol handler flush its data.
  void force_flush() { _write_buffer.flush(); }

 private:
  void _ssl_deny();
  ReadBuffer<SocketType> _read_buffer;
  WriteBuffer<SocketType> _write_buffer;
};
}  // namespace opossum
