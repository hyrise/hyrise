#include "session.hpp"

#include "client_disconnect_exception.hpp"
#include "postgres_message_type.hpp"
#include "query_handler.hpp"
#include "result_serializer.hpp"

namespace opossum {

Session::Session(boost::asio::io_service& io_service, const SendExecutionInfo send_execution_info)
    : _socket(std::make_shared<Socket>(io_service)),
      _postgres_protocol_handler(std::make_shared<PostgresProtocolHandler<Socket>>(_socket)),
      _send_execution_info(send_execution_info),
      _transaction_context(Hyrise::get().transaction_manager.new_transaction_context()) {}

std::shared_ptr<Socket> Session::socket() { return _socket; }

void Session::run() {
  // Set TCP_NODELAY in order to disable Nagle's algorithm. It handles congestion control in TCP networks. Therefore,
  // small packets are buffered and sent out later as one large packet. This might introduce a delay of up to 40 ms
  // which we have to avoid. Further reading: https://howdoesinternetwork.com/2015/nagles-algorithm
  _socket->set_option(boost::asio::ip::tcp::no_delay(true));
  _establish_connection();
  while (!_terminate_session) {
    try {
      _handle_request();
    } catch (const ClientDisconnectException&) {
      return;
    } catch (const std::exception& e) {
      std::cerr << "Exception in session with client port " << _socket->remote_endpoint().port() << ":" << std::endl
                << e.what() << std::endl;
      const auto error_message = ErrorMessage{{PostgresMessageType::HumanReadableError, e.what()}};
      _postgres_protocol_handler->send_error_message(error_message);
      _postgres_protocol_handler->send_ready_for_query();
      // In case of an error, an error message has to be send to the client followed by a "ReadyForQuery" message.
      // Messages that have already been received are processed further. A "sync" message makes the server send another
      // "ReadyForQuery" message. In order to avoid this, we set this flag for further operations. As soon as a new
      // query arrives it must be set to false again to ensure correct message flow.
      _sync_send_after_error = true;
    }
  }
}

void Session::_establish_connection() {
  const auto body_length = _postgres_protocol_handler->read_startup_packet_header();

  // Currently, the information available in the start up packet body (such as db name, user name) is ignored
  _postgres_protocol_handler->read_startup_packet_body(body_length);
  _postgres_protocol_handler->send_authentication_response();
  _postgres_protocol_handler->send_parameter("server_version", "12");
  _postgres_protocol_handler->send_parameter("server_encoding", "UTF8");
  _postgres_protocol_handler->send_parameter("client_encoding", "UTF8");
  _postgres_protocol_handler->send_parameter("DateStyle", "ISO, DMY");
  _postgres_protocol_handler->send_ready_for_query();
}

void Session::_handle_request() {
  const auto header = _postgres_protocol_handler->read_packet_type();

  switch (header) {
    case PostgresMessageType::TerminateCommand: {
      _terminate_session = true;
      break;
    }
    case PostgresMessageType::SimpleQueryCommand: {
      _sync_send_after_error = false;
      _handle_simple_query();
      break;
    }
    case PostgresMessageType::ParseCommand: {
      _sync_send_after_error = false;
      _handle_parse_command();
      break;
    }
    case PostgresMessageType::SyncCommand: {
      if (!_sync_send_after_error) {
        _sync();
      } else {
        _postgres_protocol_handler->read_sync_packet();
      }
      break;
    }
    case PostgresMessageType::BindCommand: {
      _sync_send_after_error = false;
      _handle_bind_command();
      break;
    }
    case PostgresMessageType::DescribeCommand: {
      // The contents of this packet are not used for further processing. The actual "describe" happens after
      // executing the PQP.
      _postgres_protocol_handler->read_describe_packet();
      break;
    }
    case PostgresMessageType::ExecuteCommand: {
      _handle_execute();
      break;
    }
    default:
      Fail("Unknown packet type");
  }
}

void Session::_handle_simple_query() {
  const auto& query = _postgres_protocol_handler->read_query_packet();

  // A simple query command invalidates unnamed portals
  _portals.erase("");

  const auto [execution_information, transaction_context] =
      QueryHandler::execute_pipeline(query, _send_execution_info, _transaction_context);

  if (!execution_information.error_message.empty()) {
    _transaction_context = nullptr;
    _postgres_protocol_handler->send_error_message(execution_information.error_message);
  } else {
    uint64_t row_count = 0;
    // If there is no result table, e.g. after an INSERT command, we cannot send row data. Otherwise, the result table
    // of the last statement will be send back.
    if (execution_information.result_table) {
      ResultSerializer::send_table_description(execution_information.result_table, _postgres_protocol_handler);
      ResultSerializer::send_query_response(execution_information.result_table, _postgres_protocol_handler);
      row_count = execution_information.result_table->row_count();
    }
    if (_send_execution_info == SendExecutionInfo::Yes) {
      _postgres_protocol_handler->send_execution_info(execution_information.pipeline_metrics);
    }
    _postgres_protocol_handler->send_command_complete(
        ResultSerializer::build_command_complete_message(execution_information.root_operator, row_count));

    _transaction_context = transaction_context;
  }
  _postgres_protocol_handler->send_ready_for_query();
}

void Session::_handle_parse_command() {
  const auto [statement_name, query] = _postgres_protocol_handler->read_parse_packet();
  QueryHandler::setup_prepared_plan(statement_name, query);

  _postgres_protocol_handler->send_status_message(PostgresMessageType::ParseComplete);

  // Ready for query + flush will be done after reading sync message
}

void Session::_handle_bind_command() {
  const auto parameters = _postgres_protocol_handler->read_bind_packet();

  // Named portals must be explicitly closed before they can be redefined by another Bind message,
  // but this is not required for the unnamed portal.
  // https://www.postgresql.org/docs/12/static/protocol-flow.html
  auto portal_it = _portals.find(parameters.portal);
  if (portal_it != _portals.end()) {
    AssertInput(parameters.portal.empty(), "Named portals must be explicitly closed before they can be redefined.");
    _portals.erase(portal_it);
  }

  // Since bind and execute packet usually arrive together, we still have to handle the execute packet. Therefore,
  // we first store a nullptr in the portals map to signalize an error. However, if binding succeeds in the next step
  // this nullptr gets replaced by the correct pqp. Before executing the prepared statement we make a check for errors.
  _portals.emplace(parameters.portal, nullptr);

  const auto pqp = QueryHandler::bind_prepared_plan(parameters);

  _portals[parameters.portal] = pqp;
  _postgres_protocol_handler->send_status_message(PostgresMessageType::BindComplete);

  // Ready for query + flush will be done after reading sync message
}

void Session::_sync() {
  _postgres_protocol_handler->read_sync_packet();
  if (!_transaction_context->is_auto_commit()) {
    _transaction_context->commit();
    _transaction_context.reset();
  }
  _postgres_protocol_handler->send_ready_for_query();
}

void Session::_handle_execute() {
  const std::string& portal_name = _postgres_protocol_handler->read_execute_packet();

  auto portal_it = _portals.find(portal_name);
  AssertInput(portal_it != _portals.end(), "The specified portal does not exist.");

  // In case of an error occured during binding there is no pqp available. Hence, early return here since there is
  // nothing to execute.
  if (!portal_it->second) {
    _portals.erase(portal_it);
    return;
  }

  const auto physical_plan = portal_it->second;

  if (portal_name.empty()) _portals.erase(portal_it);

  if (!_transaction_context) _transaction_context = Hyrise::get().transaction_manager.new_transaction_context();
  physical_plan->set_transaction_context_recursively(_transaction_context);

  const auto result_table = QueryHandler::execute_prepared_plan(physical_plan);

  uint64_t row_count = 0;
  // If there is no result table, e.g. after an INSERT command, we cannot send row data
  if (result_table) {
    ResultSerializer::send_table_description(result_table, _postgres_protocol_handler);
    ResultSerializer::send_query_response(result_table, _postgres_protocol_handler);
    row_count = result_table->row_count();
  } else {
    _postgres_protocol_handler->send_status_message(PostgresMessageType::NoDataResponse);
  }

  _postgres_protocol_handler->send_command_complete(
      ResultSerializer::build_command_complete_message(physical_plan->type(), row_count));
  // Ready for query + flush will be done after reading sync message
}
}  // namespace opossum
