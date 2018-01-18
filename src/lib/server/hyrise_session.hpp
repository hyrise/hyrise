#pragma once

#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <memory>
#include <optional>

#include "postgres_wire_handler.hpp"
#include "sql/sql_pipeline.hpp"
#include "types.hpp"

namespace opossum {

enum SessionState {
  Setup = 100,
  WaitingForQuery,
  ExecutingQuery,
};

using boost::asio::ip::tcp;

class AbstractCommand;

class HyriseSession : public std::enable_shared_from_this<HyriseSession> {
 public:
  static const uint32_t STARTUP_HEADER_LENGTH = 8u;
  static const uint32_t HEADER_LENGTH = 5u;

  explicit HyriseSession(tcp::socket socket, boost::asio::io_service& io_service)
      : _socket(std::move(socket)), _io_service(io_service), _input_packet(), _expected_input_packet_length(0) {}

  void start();

  void async_send_packet(OutputPacket& output_packet);

  // Interface used by Tasks
  void pipeline_created(std::unique_ptr<SQLPipeline> sql_pipeline);
  void query_executed();
  void query_response_sent();
  void load_table_file(const std::string& file_name, const std::string& table_name);

  void pipeline_error(const std::string& error_msg);
  void pipeline_info(const std::string& notice);

 protected:
  void async_receive_header(size_t size = HEADER_LENGTH);
  void async_receive_content(size_t size);
  void async_receive_packet(size_t size, bool is_header);

  void send_ssl_denied();
  void send_auth();
  void send_ready_for_query();
  void accept_query();
  void send_error(const std::string& error_msg);

  void handle_header_received(const boost::system::error_code& error, size_t bytes_transferred);
  void handle_packet_received(const boost::system::error_code& error, size_t bytes_transferred);

  void handle_packet_sent(const boost::system::error_code& error);

  void terminate_session();

  tcp::socket _socket;
  boost::asio::io_service& _io_service;
  InputPacket _input_packet;

  SessionState _state = SessionState::Setup;
  std::size_t _expected_input_packet_length;
  std::shared_ptr<HyriseSession> _self;
  std::unique_ptr<SQLPipeline> _sql_pipeline;
};

}  // namespace opossum
