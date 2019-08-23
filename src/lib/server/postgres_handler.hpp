#pragma once

#include <boost/asio.hpp>
#include <memory>
#include <vector>

#include "all_parameter_variant.hpp"
#include "buffer.hpp"
#include "network_message_types.hpp"

namespace opossum {

struct RowDescription {
  std::string column_name;
  uint64_t object_id;
  int64_t type_width;
};

struct PreparedStatementParameters {
  std::string statement_name;
  std::string portal;
  std::vector<AllTypeVariant> parameters;
};

class PostgresHandler {
 public:
    // PostgresHandler::PostgresHandler(std::shared_ptr<Socket> socket) : _read_buffer(socket), _write_buffer(socket) {}

  template <class T>
  explicit PostgresHandler(std::shared_ptr<T> socket): _read_buffer(socket), _write_buffer(socket) {}
  uint32_t read_startup_packet();
  void handle_startup_packet_body(const uint32_t size);
  void send_authentication();
  void send_parameter(const std::string& key, const std::string& value);
  void ssl_deny();
  NetworkMessageType get_packet_type();
  std::string read_query_packet();
  void command_complete(const std::string& command_complete_message);
  void send_ready_for_query();
  void send_row_description(const std::vector<RowDescription>& row_description);
  void send_data_row(const std::vector<std::string>& row_strings);
  std::pair<std::string, std::string> read_parse_packet();
  void read_sync_packet();
  void read_describe_packet();
  PreparedStatementParameters read_bind_packet();
  std::string read_execute_packet();
  void send_status_message(const NetworkMessageType message_type);
 private:
  ReadBuffer _read_buffer;
  WriteBuffer _write_buffer;
};
}  // namespace opossum
