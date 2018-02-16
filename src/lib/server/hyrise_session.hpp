#pragma once

#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/thread/future.hpp>

#include <memory>
#include <optional>

#include "postgres_wire_handler.hpp"
#include "sql/sql_pipeline.hpp"
#include "types.hpp"

namespace opossum {

enum SessionState { Setup = 100, WaitingForQuery, SimpleQuery, ExtendedQuery };

using boost::asio::ip::tcp;

class AbstractCommand;
class ClientConnection;

class HyriseSession : public std::enable_shared_from_this<HyriseSession> {
 public:
  static const uint32_t STARTUP_HEADER_LENGTH = 8u;
  static const uint32_t HEADER_LENGTH = 5u;

  explicit HyriseSession(boost::asio::io_service& io_service, std::shared_ptr<ClientConnection> connection)
      : _io_service(io_service), _connection(connection), _input_packet(), _expected_input_packet_length(0) {
    _response_buffer.reserve(_max_response_size);
  }

  void start();

  void async_send_packet(OutputPacket& output_packet);

  // Interface used by Tasks
  void pipeline_created(std::unique_ptr<SQLPipeline> sql_pipeline);
  void query_executed();
  void query_response_sent();
  void load_table_file(const std::string& file_name, const std::string& table_name);
  void prepared_bound(std::unique_ptr<SQLQueryPlan> query_plan,
                      std::shared_ptr<TransactionContext> transaction_context);
  void prepared_executed(std::shared_ptr<const Table> result_table);
  void pipeline_error(const std::string& error_msg);
  void pipeline_info(const std::string& notice);

  SessionState state() const;

 protected:
  boost::future<void> perform_session_startup();
    
  void _async_receive_header(size_t size = HEADER_LENGTH);
  void _async_receive_content(size_t size);
  void _async_receive_packet(size_t size, bool is_header);

  void _send_ssl_denied();
  void _send_auth();
  void _send_ready_for_query();
  void _send_error(const std::string& error_msg);

  void _accept_query();
  void _accept_parse();
  void _accept_bind();
  void _accept_execute();
  void _accept_sync();
  void _accept_flush();
  void _accept_describe();

  void _handle_header_received(const boost::system::error_code& error, size_t bytes_transferred);
  void _handle_packet_received(const boost::system::error_code& error, size_t bytes_transferred);

  void _handle_packet_sent(const boost::system::error_code& error);

  void _async_flush();

  void _terminate_session();

  boost::asio::io_service& _io_service;
  std::shared_ptr<ClientConnection> _connection;
  InputPacket _input_packet;
  NetworkMessageType _input_packet_type;

  // Max 2048 bytes per IP packet sent
  uint32_t _max_response_size = 2048;
  ByteBuffer _response_buffer;

  SessionState _state = SessionState::Setup;
  std::size_t _expected_input_packet_length;
  std::shared_ptr<HyriseSession> _self;
  std::unique_ptr<SQLPipeline> _sql_pipeline;

  std::unique_ptr<PreparedStatementInfo> _parse_info;
  std::unique_ptr<SQLQueryPlan> _prepared_query_plan;
  std::shared_ptr<TransactionContext> _prepared_transaction_context;
};

}  // namespace opossum
