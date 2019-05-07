#include "session.hpp"

#include <iostream>

#include "hyrise_communication.cpp"
#include "network_message_types.hpp"
#include "postgres_handler.hpp"
#include "tpch/tpch_table_generator.hpp"

namespace opossum {

Session::Session(Socket socket)
    :  _postgres_handler(std::make_shared<Socket>(std::move(socket))) {}

void Session::start() {
  _establish_connection();
  while (!_terminate_session) {
    _handle_request();
  }
}

void Session::_establish_connection() {
  const auto body_size = _postgres_handler.read_startup_packet();

  // Currently, the information available in the start up packet body (such as
  // db name, user name) is ignored
  _postgres_handler.handle_startup_packet_body(body_size);
  _postgres_handler.send_authentication();
  _postgres_handler.send_parameter("server_version", "9.5");
  _postgres_handler.send_ready_for_query();
}

void Session::_handle_request() {
  const auto header = _postgres_handler.get_packet_type();

  switch (header) {
  case NetworkMessageType::TerminateCommand: {
    _terminate_session = true;
    break;
  }
  case NetworkMessageType::SimpleQueryCommand: {
    _handle_simple_query();
    break;
  }
  case NetworkMessageType::ParseCommand: {
    _handle_parse_command();
    break;
  }
  default:
    std::cout << "Unknown packet type" << std::endl;
  }
}

void Session::_handle_simple_query() {
    const auto& query = _postgres_handler.read_packet_body();

    auto sql_pipeline = create_pipeline(query);
    execute_pipeline(sql_pipeline);
    auto row_description = build_row_description(sql_pipeline);
    auto row_count = 0u;
    if (!row_description.empty()) {
      _postgres_handler.send_row_description(row_description);
      row_count = send_query_response(sql_pipeline, _postgres_handler);
    }
    _postgres_handler.command_complete(build_command_complete_message(sql_pipeline, row_count));
    _postgres_handler.send_ready_for_query();
}

void Session::_handle_parse_command() {
}
}  // namespace opossum
