#pragma once

#include <unordered_map>

#include "all_type_variant.hpp"
#include "postgres_message_type.hpp"
#include "read_buffer.hpp"
#include "write_buffer.hpp"

namespace opossum {

using ErrorMessage = std::unordered_map<PostgresMessageType, std::string>;

// This struct stores a prepared statement's name, its portal used and the specified parameters.
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

  // Handle the startup packet header returning the body's size
  uint32_t read_startup_packet_header();
  void read_startup_packet_body(const uint32_t size);

  // Setup new connection: successful authentication + sending parameters
  void send_authentication_response();
  void send_parameter(const std::string& key, const std::string& value);

  // Ready to receive a new packet
  void send_ready_for_query();

  // Read first byte of next packet to determine its type
  PostgresMessageType read_packet_type();

  // Read SQL query packet
  std::string read_query_packet();

  // Send query result
  void send_row_description_header(const uint32_t total_column_name_length, const uint16_t column_count);
  void send_row_description(const std::string& column_name, const uint32_t object_id, const int16_t type_width);
  void send_data_row(const std::vector<std::optional<std::string>>& values_as_strings,
                     const uint32_t string_length_sum);
  void send_command_complete(const std::string& command_complete_message);

  // Messages for parsing prepared statements
  std::pair<std::string, std::string> read_parse_packet();
  void read_sync_packet();

  // Send out status message containing PostgresMessageType and length
  void send_status_message(const PostgresMessageType message_type);

  // Series of packets for binding and executing prepared statements
  void read_describe_packet();
  PreparedStatementDetails read_bind_packet();
  std::string read_execute_packet();

  // Send error message to client if there is an error during parsing or execution
  void send_error_message(const ErrorMessage& error_message);

  // Additional (optional) message containing execution times of different components (such as translator or optimizer)
  void send_execution_info(const std::string& execution_information);

  // This method is required for testing. Otherwise we cannot make the protocol handler flush its data.
  void force_flush() { _write_buffer.flush(); }

 private:
  void _ssl_deny();
  ReadBuffer<SocketType> _read_buffer;
  WriteBuffer<SocketType> _write_buffer;
};
}  // namespace opossum
