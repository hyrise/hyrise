#pragma once

#include <boost/asio.hpp>

#include "all_parameter_variant.hpp"
#include "buffer.hpp"
#include "network_message_types.hpp"

namespace opossum {

struct PreparedStatementDetails {
  std::string statement_name;
  std::string portal;
  std::vector<AllTypeVariant> parameters;
};

class PostgresProtocolHandler {
 public:
  explicit PostgresProtocolHandler(const std::shared_ptr<Socket> socket);
  uint32_t read_startup_packet();
  void handle_startup_packet_body(const uint32_t size);
  void send_authentication();
  void send_parameter(const std::string& key, const std::string& value);
  NetworkMessageType get_packet_type();
  std::string read_query_packet();
  void command_complete(const std::string& command_complete_message);
  void send_ready_for_query();
  void set_row_description_header(const uint32_t total_column_name_length, const uint16_t column_count);
  void send_row_description(const std::string& column_name, const uint32_t object_id, const int16_t type_width);
  void send_data_row(const std::vector<std::string>& row_strings);
  std::pair<std::string, std::string> read_parse_packet();
  void read_sync_packet();
  void read_describe_packet();
  PreparedStatementDetails read_bind_packet();
  std::string read_execute_packet();
  void send_status_message(const NetworkMessageType message_type);

 private:
  void _ssl_deny();

  ReadBuffer _read_buffer;
  WriteBuffer _write_buffer;
};
}  // namespace opossum
