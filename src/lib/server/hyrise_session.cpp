#include "hyrise_session.hpp"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/asio/placeholders.hpp>
#include <boost/asio/write.hpp>
#include <boost/bind.hpp>

#include <chrono>
#include <iostream>
#include <thread>
#include <sql/sql_translator.hpp>
#include <tasks/server/execute_server_prepared_statement.hpp>
#include <tasks/server/bind_server_prepared_statement.hpp>

#include "SQLParserResult.h"
#include "scheduler/current_scheduler.hpp"
#include "sql/sql_pipeline.hpp"
#include "tasks/server/create_pipeline_task.hpp"
#include "tasks/server/execute_server_query_task.hpp"
#include "tasks/server/load_server_file_task.hpp"
#include "tasks/server/send_query_response_task.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/load_table.hpp"
#include "concurrency/transaction_manager.hpp"

namespace opossum {

void HyriseSession::start() {
  // Keep a pointer to itself that will be released once the connection is closed
  _self = shared_from_this();
  _async_receive_header(STARTUP_HEADER_LENGTH);
}

void HyriseSession::async_send_packet(OutputPacket& output_packet) {
  // If the packet is SslNo (size == 1), it has a special format and does not require a size
  if (output_packet.data.size() > 1) {
    PostgresWireHandler::write_output_packet_size(output_packet);
  }

  if (_response_buffer.size() + output_packet.data.size() > _max_response_size) {
    _async_flush();
  }

  _response_buffer.insert(_response_buffer.end(), output_packet.data.begin(), output_packet.data.end());

//  boost::asio::async_write(_socket, boost::asio::buffer(output_packet.data),
//                           boost::bind(&HyriseSession::_handle_packet_sent, this, boost::asio::placeholders::error));
}

void HyriseSession::_handle_header_received(const boost::system::error_code& error, size_t bytes_transferred) {
  if (error) {
    std::cout << error.category().name() << ':' << error.value() << std::endl;
    Fail("An error occurred while reading from the connection.");
  }

  Assert(bytes_transferred == _expected_input_packet_length, "Client sent less data than expected.");

  if (_state == SessionState::Setup) {
    auto startup_packet_length = PostgresWireHandler::handle_startup_package(_input_packet);
    if (startup_packet_length == 0) {
      // Handle SSL packet
      return _send_ssl_denied();
    }
    // Read content of this packet
    return _async_receive_content(startup_packet_length);
  }

  // We're currently idling, so read a new incoming message header
  auto command_header = PostgresWireHandler::handle_header(_input_packet);
  _input_packet_type = command_header.message_type;
  if (command_header.message_type == NetworkMessageType::TerminateCommand) {
    // This immediately releases the session object
    return _terminate_session();
  }

  return _async_receive_content(command_header.payload_length);
}

void HyriseSession::_handle_packet_received(const boost::system::error_code& error, size_t bytes_transferred) {
  if (error) {
    std::cout << error.category().name() << ':' << error.value() << std::endl;
    Fail("An error occurred while reading from the connection.");
  }

  Assert(bytes_transferred == _expected_input_packet_length, "Client sent less data than expected.");

  if (_state == SessionState::Setup) {
    // Read these values and ignore them
    PostgresWireHandler::handle_startup_package_content(_input_packet, bytes_transferred);
    return _send_auth();
  }

  // We're currently waiting for a query, so accept the incoming one
  switch (_input_packet_type) {
    case NetworkMessageType::SimpleQueryCommand:
      _accept_query();
      break;
    case NetworkMessageType::ParseCommand:
      _accept_parse();
      break;
    case NetworkMessageType::BindCommand:
      _accept_bind();
      break;
    case NetworkMessageType::DescribeCommand:
      _accept_describe();
      break;
    case NetworkMessageType::SyncCommand:
      _accept_sync();
      break;
    case NetworkMessageType::FlushCommand:
      _accept_flush();
      break;
    case NetworkMessageType::ExecuteCommand:
      _accept_execute();
      break;
    default:
      _send_error("Unsupported message type");
      _send_ready_for_query();
  }
}

void HyriseSession::_terminate_session() {
  _socket.close();
  _self.reset();
}

void HyriseSession::_handle_packet_sent(const boost::system::error_code& error) {
  if (error) {
    std::cout << error.category().name() << ':' << error.value() << std::endl;
    Fail("An error occurred when writing to the connection");
  }

  _response_buffer.clear();
}

void HyriseSession::_async_receive_header(size_t size) {
  return _async_receive_packet(size, /* is_header = */ true);
}

void HyriseSession::_async_receive_content(size_t size) {
  return _async_receive_packet(size, /* is_header = */ false);
}

void HyriseSession::_async_receive_packet(size_t size, bool is_header) {
  _expected_input_packet_length = size;
  _input_packet.offset = _input_packet.data.begin();

  auto next_handler = is_header ? &HyriseSession::_handle_header_received : &HyriseSession::_handle_packet_received;

  _socket.async_read_some(
      boost::asio::buffer(_input_packet.data, size),
      boost::bind(next_handler, this, boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred));
}

void HyriseSession::_send_ssl_denied() {
  OutputPacket output_packet;
  PostgresWireHandler::write_value(output_packet, NetworkMessageType::SslNo);
  async_send_packet(output_packet);
  _async_flush();

  _async_receive_header(STARTUP_HEADER_LENGTH);
}

void HyriseSession::_send_auth() {
  // This packet is our AuthenticationOK, which means we do not require any auth.
  OutputPacket output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::AuthenticationRequest);
  PostgresWireHandler::write_value(output_packet, htonl(0u));
  async_send_packet(output_packet);

  _send_ready_for_query();
}

void HyriseSession::_send_ready_for_query() {
  _state = SessionState::WaitingForQuery;
  // ReadyForQuery packet 'Z' with transaction status Idle 'I'
  OutputPacket output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::ReadyForQuery);
  PostgresWireHandler::write_value(output_packet, TransactionStatusIndicator::Idle);
  async_send_packet(output_packet);
  _async_flush();

  // Now we wait for the next query to come
  _async_receive_header();
}

void HyriseSession::_accept_query() {
  const auto sql = PostgresWireHandler::handle_query_packet(_input_packet, _expected_input_packet_length);
  const std::vector<std::shared_ptr<ServerTask>> tasks = {std::make_shared<CreatePipelineTask>(_self, sql)};
  _state = SessionState::ExecutingQuery;
  CurrentScheduler::schedule_tasks(tasks);
}

void HyriseSession::_accept_parse() {
  _parse_info = std::make_unique<PreparedStatementInfo>(PostgresWireHandler::handle_parse_packet(_input_packet, _expected_input_packet_length));
  const std::vector<std::shared_ptr<ServerTask>> tasks = {std::make_shared<CreatePipelineTask>(_self, _parse_info->query)};
  _state = SessionState::PreparingStatement;
  CurrentScheduler::schedule_tasks(tasks);
}

void HyriseSession::_accept_bind() {
  auto params = PostgresWireHandler::handle_bind_packet(_input_packet, _expected_input_packet_length);
  _state = SessionState::BindingStatement;

  const std::vector<std::shared_ptr<ServerTask>> tasks = {std::make_shared<BindServerPreparedStatement>(_self, _sql_pipeline, std::move(params))};
  CurrentScheduler::schedule_tasks(tasks);
}

void HyriseSession::_accept_execute() {
  const auto portal = PostgresWireHandler::handle_execute_packet(_input_packet, _expected_input_packet_length);

  const std::vector<std::shared_ptr<ServerTask>> tasks =
    {std::make_shared<ExecuteServerPreparedStatement>(_self, std::move(_prepared_query_plan), std::move(_prepared_transaction_context))};
  CurrentScheduler::schedule_tasks(tasks);
}

void HyriseSession::_accept_sync() {
  PostgresWireHandler::read_values<char>(_input_packet, _expected_input_packet_length);
  return _async_receive_header();
}

void HyriseSession::_accept_flush() {
  PostgresWireHandler::read_values<char>(_input_packet, _expected_input_packet_length);
  return _async_receive_header();
}

void HyriseSession::_accept_describe() {
  PostgresWireHandler::handle_describe_packet(_input_packet, _expected_input_packet_length);
  return _async_receive_header();
}

void HyriseSession::_send_error(const std::string& message) {
  OutputPacket output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::ErrorResponse);

  // An error response has to include at least one identified field

  // Send the error message
  PostgresWireHandler::write_value(output_packet, 'M');
  PostgresWireHandler::write_string(output_packet, message);

  // Terminate the error response
  PostgresWireHandler::write_value(output_packet, '\0');
  async_send_packet(output_packet);
  _async_flush();
}

void HyriseSession::pipeline_info(const std::string& notice) {
  OutputPacket output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::Notice);

  PostgresWireHandler::write_value(output_packet, 'M');
  PostgresWireHandler::write_string(output_packet, notice);

  // Terminate the notice response
  PostgresWireHandler::write_value(output_packet, '\0');
  async_send_packet(output_packet);
}

void HyriseSession::pipeline_created(std::unique_ptr<SQLPipeline> sql_pipeline) {
  _sql_pipeline = std::move(sql_pipeline);

  if (_state == SessionState::PreparingStatement) {
    auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::ParseComplete);
    async_send_packet(output_packet);
    return _async_receive_header();
  }

  std::vector<std::shared_ptr<ServerTask>> tasks = {std::make_shared<ExecuteServerQueryTask>(_self, *_sql_pipeline)};
  CurrentScheduler::schedule_tasks(tasks);
}

void HyriseSession::query_executed() {
  const std::vector<std::shared_ptr<SendQueryResponseTask>> tasks = {
      std::make_shared<SendQueryResponseTask>(_self, *_sql_pipeline, _sql_pipeline->get_result_table())};
  CurrentScheduler::schedule_tasks(tasks);
}

void HyriseSession::prepared_bound(std::unique_ptr<SQLQueryPlan> query_plan,
                                   std::shared_ptr<TransactionContext> transaction_context) {
  _prepared_query_plan = std::move(query_plan);
  _prepared_transaction_context = std::move(transaction_context);
  auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::BindComplete);
  async_send_packet(output_packet);
  return _async_receive_header();
}

void HyriseSession::prepared_executed(std::shared_ptr<const Table> result_table) {
  const std::vector<std::shared_ptr<SendQueryResponseTask>> tasks = {
    std::make_shared<SendQueryResponseTask>(_self, *_sql_pipeline, std::move(result_table))};
  CurrentScheduler::schedule_tasks(tasks);
}

void HyriseSession::query_response_sent() { _send_ready_for_query(); }

void HyriseSession::load_table_file(const std::string& file_name, const std::string& table_name) {
  const std::vector<std::shared_ptr<LoadServerFileTask>> tasks = {
      std::make_shared<LoadServerFileTask>(_self, file_name, table_name)};
  CurrentScheduler::schedule_tasks(tasks);
}

void HyriseSession::pipeline_error(const std::string& error_msg) {
  _send_error(error_msg);
  _send_ready_for_query();
}

bool HyriseSession::is_extended_query_mode() const {
  return _state == SessionState::PreparingStatement || _state == SessionState::BindingStatement || _state == SessionState::ExecutingQuery;
};


void HyriseSession::_async_flush() {
  boost::asio::async_write(_socket, boost::asio::buffer(_response_buffer),
                           boost::bind(&HyriseSession::_handle_packet_sent, this, boost::asio::placeholders::error));
}

}  // namespace opossum
