#include "hyrise_session.hpp"

#include <boost/asio/placeholders.hpp>
#include <boost/asio/write.hpp>
#include <boost/bind.hpp>

#include <iostream>

#include "scheduler/current_scheduler.hpp"
#include "sql/sql_pipeline.hpp"
#include "tasks/server/create_pipeline_task.hpp"
#include "tasks/server/execute_server_query_task.hpp"
#include "tasks/server/send_query_response_task.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

void HyriseSession::start() {
  // Keep a pointer to itself that will be released once the connection is closed
  _self = shared_from_this();
  async_receive_header(STARTUP_HEADER_LENGTH);
}

void HyriseSession::async_send_packet(OutputPacket& output_packet) {
  // If the packet is SslNo (size == 1), it has a special format and does not require a size
  if (output_packet.data.size() > 1) {
    PostgresWireHandler::write_output_packet_size(output_packet);
  }

  boost::asio::async_write(_socket, boost::asio::buffer(output_packet.data),
                           boost::bind(&HyriseSession::handle_packet_sent, this, boost::asio::placeholders::error));
}

void HyriseSession::handle_header_received(const boost::system::error_code& error, size_t bytes_transferred) {
  if (error) {
    std::cout << error.category().name() << ':' << error.value() << std::endl;
    Fail("An error occurred while reading from the connection.");
  }

  Assert(bytes_transferred == _expected_input_packet_length, "Client sent less data than expected.");

  if (_state == SessionState::Setup) {
    auto startup_packet_length = PostgresWireHandler::handle_startup_package(_input_packet);
    if (startup_packet_length == 0) {
      // Handle SSL packet
      return send_ssl_denied();
    }
    // Read content of this packet
    return async_receive_content(startup_packet_length);
  }

  // We're currently idling, so read a new incoming message header
  auto command_header = PostgresWireHandler::handle_header(_input_packet);
  if (command_header.message_type == NetworkMessageType::TerminateCommand) {
    // This immediately releases the session object
    return terminate_session();
  }

  return async_receive_content(command_header.payload_length);
}

void HyriseSession::handle_packet_received(const boost::system::error_code& error, size_t bytes_transferred) {
  if (error) {
    std::cout << error.category().name() << ':' << error.value() << std::endl;
    Fail("An error occurred while reading from the connection.");
  }

  Assert(bytes_transferred == _expected_input_packet_length, "Client sent less data than expected.");

  if (_state == SessionState::Setup) {
    // Read these values and ignore them
    PostgresWireHandler::handle_startup_package_content(_input_packet, bytes_transferred);
    return send_auth();
  }

  // We're currently waiting for a query, so accept the incoming one
  return accept_query();
}

void HyriseSession::terminate_session() {
  _socket.close();
  _self.reset();
}

void HyriseSession::handle_packet_sent(const boost::system::error_code& error) {
  if (error) {
    std::cout << error.category().name() << ':' << error.value() << std::endl;
    Fail("An error occurred when writing to the connection");
  }
}

void HyriseSession::async_receive_header(const size_t size) {
  return async_receive_packet(size, /* is_header = */ true);
}

void HyriseSession::async_receive_content(const size_t size) {
  return async_receive_packet(size, /* is_header = */ false);
}

void HyriseSession::async_receive_packet(const size_t size, const bool is_header) {
  _expected_input_packet_length = size;
  _input_packet.offset = _input_packet.data.begin();

  auto next_handler = is_header ? &HyriseSession::handle_header_received : &HyriseSession::handle_packet_received;

  _socket.async_read_some(
      boost::asio::buffer(_input_packet.data, size),
      boost::bind(next_handler, this, boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred));
}

void HyriseSession::send_ssl_denied() {
  OutputPacket output_packet;
  PostgresWireHandler::write_value(output_packet, NetworkMessageType::SslNo);
  async_send_packet(output_packet);
  async_receive_header(STARTUP_HEADER_LENGTH);
}

void HyriseSession::send_auth() {
  // This packet is our AuthenticationOK, which means we do not require any auth.
  OutputPacket output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::AuthenticationRequest);
  PostgresWireHandler::write_value(output_packet, htonl(0u));
  async_send_packet(output_packet);

  send_ready_for_query();
}

void HyriseSession::send_ready_for_query() {
  _state = SessionState::WaitingForQuery;
  // ReadyForQuery packet 'Z' with transaction status Idle 'I'
  OutputPacket output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::ReadyForQuery);
  PostgresWireHandler::write_value(output_packet, TransactionStatusIndicator::Idle);
  async_send_packet(output_packet);

  // Now we wait for the next query to come
  async_receive_header();
}

void HyriseSession::accept_query() {
  const auto sql = PostgresWireHandler::handle_query_packet(_input_packet, _expected_input_packet_length);
  const std::vector<std::shared_ptr<ServerTask>> tasks = {std::make_shared<CreatePipelineTask>(_self, sql)};
  _state = SessionState::ExecutingQuery;
  CurrentScheduler::schedule_tasks(tasks);
}

void HyriseSession::send_error(const std::string& message) {
  OutputPacket output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::ErrorResponse);
  
  // An error response has to include at least one identified field
  
  // Send the error message
  PostgresWireHandler::write_value(output_packet, 'M');
  PostgresWireHandler::write_string(output_packet, message);
  
  // Terminate the error response
  PostgresWireHandler::write_value(output_packet, '\0');
  
  async_send_packet(output_packet);
}

void HyriseSession::pipeline_created(std::unique_ptr<SQLPipeline> sql_pipeline) {
  _sql_pipeline = std::move(sql_pipeline);

  const std::vector<std::shared_ptr<ServerTask>> tasks = {
      std::make_shared<ExecuteServerQueryTask>(_self, *_sql_pipeline)};
  CurrentScheduler::schedule_tasks(tasks);
}

void HyriseSession::query_executed() {
  const std::vector<std::shared_ptr<ServerTask>> tasks = {
      std::make_shared<SendQueryResponseTask>(_self, *_sql_pipeline)};
  CurrentScheduler::schedule_tasks(tasks);
}

void HyriseSession::query_response_sent() { send_ready_for_query(); }

void HyriseSession::pipeline_error(const std::string& error_msg) {
  send_error(error_msg);
  send_ready_for_query();
}

}  // namespace opossum
