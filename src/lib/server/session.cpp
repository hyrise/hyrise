#include "session.hpp"

#include "hyrise_communicator.hpp"
#include "network_message_types.hpp"
#include "response_builder.hpp"

namespace opossum {

Session::Session(Socket socket)
    : _socket(std::make_shared<Socket>(std::move(socket))),
      _postgres_handler(std::make_shared<PostgresHandler>(_socket)) {
  _socket->set_option(boost::asio::ip::tcp::no_delay(true));
}

void Session::start() {
  _establish_connection();
  while (!_terminate_session) {
    _handle_request();
  }
}

void Session::_establish_connection() {
  const auto body_size = _postgres_handler->read_startup_packet();

  // Currently, the information available in the start up packet body (such as db name, user name) is ignored
  _postgres_handler->handle_startup_packet_body(body_size);
  _postgres_handler->send_authentication();
  _postgres_handler->send_parameter("server_version", "11");
  _postgres_handler->send_parameter("server_encoding", "UTF8");
  _postgres_handler->send_parameter("client_encoding", "UTF8");
  _postgres_handler->send_ready_for_query();
}

void Session::_handle_request() {
  const auto header = _postgres_handler->get_packet_type();

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
    case NetworkMessageType::SyncCommand: {
      _sync();
      break;
    }
    case NetworkMessageType::BindCommand: {
      _handle_bind_command();
      break;
    }
    case NetworkMessageType::DescribeCommand: {
      _handle_describe();
      break;
    }
    case NetworkMessageType::ExecuteCommand: {
      _handle_execute();
      break;
    }
    default:
      Fail("Unknown packet type");
  }
}

void Session::_handle_simple_query() {
  const auto& query = _postgres_handler->read_query_packet();

  // A simple query command invalidates unnamed portals
  _portals.erase("");

  const auto [result_table, root_operator_type] = HyriseCommunicator::execute_pipeline(query);

  auto row_count = 0;
  // If there is no result table, e.g. after an INSERT command, we cannot send row data
  if (result_table) {
    ResponseBuilder::build_row_description(result_table, _postgres_handler);
    // TODO(toni): Is that the right place?
    HyriseCommunicator::send_query_response(result_table, _postgres_handler);
    row_count = result_table->row_count();
  }

  _postgres_handler->command_complete(ResponseBuilder::build_command_complete_message(root_operator_type, row_count));
  _postgres_handler->send_ready_for_query();
}

void Session::_handle_parse_command() {
  const auto [statement_name, query] = _postgres_handler->read_parse_packet();

  HyriseCommunicator::setup_prepared_plan(statement_name, query);

  _postgres_handler->send_status_message(NetworkMessageType::ParseComplete);
  // Ready for query + flush will be done after reading sync message
}

void Session::_handle_bind_command() {
  const auto parameters = _postgres_handler->read_bind_packet();

  // Named portals must be explicitly closed before they can be redefined by another Bind message,
  // but this is not required for the unnamed portal.
  // https://www.postgresql.org/docs/10/static/protocol-flow.html
  auto portal_it = _portals.find(parameters.portal);
  if (portal_it != _portals.end()) {
    Assert(parameters.portal.empty(), "Named portals must be explicitly closed before they can be redefined.");
    _portals.erase(portal_it);
  }

  const auto& physical_plan = HyriseCommunicator::bind_prepared_plan(parameters);
  _portals.emplace(parameters.portal, physical_plan);

  _postgres_handler->send_status_message(NetworkMessageType::BindComplete);
  // Ready for query + flush will be done after reading sync message
}

void Session::_handle_describe() { _postgres_handler->read_describe_packet(); }

void Session::_sync() {
  _postgres_handler->read_sync_packet();
  if (_transaction) {
    _transaction->commit();
    _transaction.reset();
  }
  _postgres_handler->send_ready_for_query();
}

void Session::_handle_execute() {
  const std::string& portal_name = _postgres_handler->read_execute_packet();
  auto portal_it = _portals.find(portal_name);
  Assert(portal_it != _portals.end(), "The specified portal does not exist.");

  const auto physical_plan = portal_it->second;

  // TODO(toni): WTH
  if (portal_name.empty()) _portals.erase(portal_it);

  if (!_transaction) _transaction = HyriseCommunicator::get_new_transaction_context();
  physical_plan->set_transaction_context_recursively(_transaction);

  const auto result_table = HyriseCommunicator::execute_prepared_statement(physical_plan);

  auto row_count = 0;
  // If there is no result table, e.g. after an INSERT command, we cannot send row data
  if (result_table) {
    ResponseBuilder::build_row_description(result_table, _postgres_handler);
    // TODO(toni): Is that the right place?
    HyriseCommunicator::send_query_response(result_table, _postgres_handler);
    row_count = result_table->row_count();
  } else {
    _postgres_handler->send_status_message(NetworkMessageType::NoDataResponse);
  }

  _postgres_handler->command_complete(
      ResponseBuilder::build_command_complete_message(physical_plan->type(), row_count));
  // Ready for query + flush will be done after reading sync message
}
}  // namespace opossum
