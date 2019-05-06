#pragma once

#include <boost/asio.hpp>
#include <memory>
#include <vector>

#include "buffer.hpp"
#include "network_message_types.hpp"

namespace opossum {

// Copy paste
struct RowDescription {
  std::string column_name;
  uint64_t object_id;
  int64_t type_width;
};

class PostgresHandler {
 public:
  explicit PostgresHandler(boost::asio::ip::tcp::socket socket);
  uint32_t read_startup_packet();
  void handle_startup_packet_body(const uint32_t size);
  void send_authentication();
  void send_parameter(const std::string& key, const std::string& value);
  void ssl_deny();
  NetworkMessageType get_packet_type();
  const std::string read_packet_body();
  void command_complete(const std::string& command_complete_message);
  void send_ready_for_query();
  void send_row_description(const std::vector<RowDescription>& row_description);
  void send_data_row(const std::vector<std::string>& row_strings);

 private:
  ReadBuffer _read_buffer;
  WriteBuffer _write_buffer;
};
}  // namespace opossum
