#include "server_session.hpp"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/asio/placeholders.hpp>
#include <boost/asio/write.hpp>
#include <boost/bind.hpp>

#include <chrono>
#include <iostream>
#include <thread>

#include "SQLParserResult.h"

#include "concurrency/transaction_manager.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_translator.hpp"
#include "tasks/server/bind_server_prepared_statement_task.hpp"
#include "tasks/server/create_pipeline_task.hpp"
#include "tasks/server/execute_server_prepared_statement_task.hpp"
#include "tasks/server/execute_server_query_task.hpp"
#include "tasks/server/load_server_file_task.hpp"

#include "client_connection.hpp"
#include "query_response_builder.hpp"
#include "then_operator.hpp"
#include "types.hpp"
#include "use_boost_future.hpp"
#include "utils/assert.hpp"
#include "utils/load_table.hpp"

namespace opossum {

using opossum::then_operator::then;

template <typename TConnection, typename TTaskRunner>
boost::future<void> ServerSessionImpl<TConnection, TTaskRunner>::start() {
  // We need a copy of this session to outlive the async operation
  auto self = this->shared_from_this();
  return (_perform_session_startup() >> then >> [this, self]() { return _handle_client_requests(); })
      // Use .then instead of >> then >> to be able to handle exceptions
      .then(boost::launch::sync, [self](boost::future<void> f) {
        try {
          f.get();
        } catch (const std::exception& e) {
          std::cerr << e.what() << std::endl;
        }
      });
}

template <typename TConnection, typename TTaskRunner>
boost::future<void> ServerSessionImpl<TConnection, TTaskRunner>::_perform_session_startup() {
  return _connection->receive_startup_packet_header() >> then >> [=](uint32_t startup_packet_length) {
    if (startup_packet_length == 0) {
      // This is a request for SSL, deny it and wait for the next startup packet
      return _connection->send_ssl_denied() >> then >> [=]() { return _perform_session_startup(); };
    }

    return _connection->receive_startup_packet_body(startup_packet_length) >> then >>
           [=]() { return _connection->send_auth(); } >> then >>
           // We need to provide some random server version > 9 here, because some clients require it.
           [=]() { return _connection->send_parameter_status("server_version", "9.5"); } >> then >>
           [=]() { return _connection->send_ready_for_query(); };
  };
}

template <typename TConnection, typename TTaskRunner>
boost::future<void> ServerSessionImpl<TConnection, TTaskRunner>::_handle_client_requests() {
  auto process_command = [=](RequestHeader request) {
    switch (request.message_type) {
      case NetworkMessageType::SimpleQueryCommand: {
        return _connection->receive_simple_query_packet_body(request.payload_length) >> then >>
               [=](std::string sql) { return _handle_simple_query_command(sql); } >> then >>
               [=]() { return _connection->send_ready_for_query(); };
      }

      case NetworkMessageType::ParseCommand: {
        return _connection->receive_parse_packet_body(request.payload_length) >> then >>
               [=](ParsePacket parse_packet) { return _handle_parse_command(parse_packet); };
      }

      case NetworkMessageType::BindCommand: {
        return _connection->receive_bind_packet_body(request.payload_length) >> then >>
               [=](BindPacket bind_packet) { return _handle_bind_command(bind_packet); };
      }

      case NetworkMessageType::DescribeCommand: {
        return _connection->receive_describe_packet_body(request.payload_length) >> then >>
               [=](std::string portal) { return _handle_describe_command(portal); };
      }

      case NetworkMessageType::SyncCommand: {
        return _connection->receive_sync_packet_body(request.payload_length) >> then >>
               [=]() { return _handle_sync_command(); } >> then >>
               [=]() { return _connection->send_ready_for_query(); };
      }

      case NetworkMessageType::FlushCommand: {
        return _connection->receive_flush_packet_body(request.payload_length) >> then >>
               [=]() { return _handle_flush_command(); };
      }

      case NetworkMessageType::ExecuteCommand: {
        return _connection->receive_execute_packet_body(request.payload_length) >> then >>
               [=](std::string portal) { return _handle_execute_command(portal); };
      }

      default:
        Fail("Unsupported message type.");
    }
  };

  // We need a copy of this session to outlive the async operation
  auto self = this->shared_from_this();
  return _connection->receive_packet_header() >> then >> [this, self, process_command](RequestHeader request) {
    if (request.message_type == NetworkMessageType::TerminateCommand) {
      return boost::make_ready_future();
    }

    // Handle any exceptions that have occurred during process_command. For this, we need to call .then() explicitly,
    // because >> then >> does not handle exceptions
    return process_command(request)
               .then(boost::launch::sync,
                     [this, self](boost::future<void> result) {
                       try {
                         result.get();
                         return boost::make_ready_future();
                       } catch (const std::exception& e) {
                         // Abort the current transaction
                         if (_transaction) {
                           _transaction->rollback();
                           _transaction.reset();
                         }

                         return _connection->send_error(e.what()) >> then >>
                                [this, self]() { return _connection->send_ready_for_query(); };
                       }
                     })
               .unwrap()
           // Proceed with the next incoming message
           >> then >> [this, self]() { return _handle_client_requests(); };
  };
}

template <typename TConnection, typename TTaskRunner>
boost::future<void> ServerSessionImpl<TConnection, TTaskRunner>::_send_simple_query_response(
    const std::shared_ptr<SQLPipeline>& sql_pipeline) {
  auto result_table = sql_pipeline->get_result_table();

  auto send_row_data = [=]() {
    // If there is no result table, e.g. after an INSERT command, we cannot send row data
    if (!result_table) return boost::make_ready_future<uint64_t>(0);

    auto row_description = QueryResponseBuilder::build_row_description(sql_pipeline->get_result_table());

    return _connection->send_row_description(row_description) >> then >> [=]() {
      return QueryResponseBuilder::send_query_response(
          [=](const std::vector<std::string>& row) { return _connection->send_data_row(row); }, *result_table);
    };
  };

  auto send_command_complete = [=](uint64_t row_count) {
    auto statement_type = sql_pipeline->get_parsed_sql_statements().front()->getStatements().front()->type();
    auto complete_message = QueryResponseBuilder::build_command_complete_message(statement_type, row_count);
    return _connection->send_command_complete(complete_message);
  };

  return send_row_data() >> then >> send_command_complete >> then >> [=]() {
    auto execution_info = QueryResponseBuilder::build_execution_info_message(sql_pipeline);
    return _connection->send_notice(execution_info);
  };
}

template <typename TConnection, typename TTaskRunner>
boost::future<void> ServerSessionImpl<TConnection, TTaskRunner>::_handle_simple_query_command(const std::string& sql) {
  auto create_sql_pipeline = [=]() {
    return _task_runner->dispatch_server_task(std::make_shared<CreatePipelineTask>(sql, true));
  };

  auto load_table_file = [=](std::string& file_name, std::string& table_name) {
    auto task = std::make_shared<LoadServerFileTask>(file_name, table_name);
    return _task_runner->dispatch_server_task(task) >> then >>
           [=]() { return _connection->send_notice("Successfully loaded " + table_name); };
  };

  auto execute_sql_pipeline = [=](std::shared_ptr<SQLPipeline> sql_pipeline) {
    auto task = std::make_shared<ExecuteServerQueryTask>(sql_pipeline);
    return _task_runner->dispatch_server_task(task) >> then >> [=]() { return sql_pipeline; };
  };

  // A simple query command invalidates unnamed statements and portals
  _prepared_statements.erase("");
  _portals.erase("");

  return create_sql_pipeline() >> then >> [=](std::unique_ptr<CreatePipelineResult> result) {
    if (result->load_table.has_value()) {
      return load_table_file(result->load_table->first, result->load_table->second);
    } else {
      return execute_sql_pipeline(result->sql_pipeline) >> then >>
             [=](std::shared_ptr<SQLPipeline> sql_pipeline) { return _send_simple_query_response(sql_pipeline); };
    }
  };
}

template <typename TConnection, typename TTaskRunner>
boost::future<void> ServerSessionImpl<TConnection, TTaskRunner>::_handle_parse_command(const ParsePacket& parse_info) {
  auto prepared_statement_name = parse_info.statement_name;

  // Named prepared statements must be explicitly closed before they can be redefined by another Parse message
  // https://www.postgresql.org/docs/10/static/protocol-flow.html
  auto statement_it = _prepared_statements.find(prepared_statement_name);
  if (statement_it != _prepared_statements.end()) {
    if (!prepared_statement_name.empty()) {
      Fail("Named prepared statements must be explicitly closed before they can be redefined.");
    }
    _prepared_statements.erase(statement_it);
  }

  return _task_runner->dispatch_server_task(std::make_shared<CreatePipelineTask>(parse_info.query)) >> then >>
         [=](std::unique_ptr<CreatePipelineResult> result) {
           // We know that SQLPipeline is set because the load table command is not allowed in this context
           _prepared_statements.insert(std::make_pair(prepared_statement_name, result->sql_pipeline));
         } >>
         then >> [=]() { return _connection->send_status_message(NetworkMessageType::ParseComplete); };
}

template <typename TConnection, typename TTaskRunner>
boost::future<void> ServerSessionImpl<TConnection, TTaskRunner>::_handle_bind_command(const BindPacket& packet) {
  auto statement_it = _prepared_statements.find(packet.statement_name);
  if (statement_it == _prepared_statements.end()) Fail("The specified statement does not exist.");

  auto sql_pipeline = statement_it->second;
  if (packet.statement_name.empty()) _prepared_statements.erase(statement_it);

  auto portal_name = packet.destination_portal;

  // Named portals must be explicitly closed before they can be redefined by another Bind message,
  // but this is not required for the unnamed portal.
  // https://www.postgresql.org/docs/10/static/protocol-flow.html
  auto portal_it = _portals.find(portal_name);
  if (portal_it != _portals.end()) {
    if (!portal_name.empty()) Fail("Named portals must be explicitly closed before they can be redefined.");
    _portals.erase(portal_it);
  }

  auto statement_type = sql_pipeline->get_parsed_sql_statements().front()->getStatements().front()->type();

  auto task = std::make_shared<BindServerPreparedStatementTask>(sql_pipeline, packet.params);
  return _task_runner->dispatch_server_task(task) >> then >>
         [=](std::unique_ptr<SQLQueryPlan> query_plan) {
           std::shared_ptr<SQLQueryPlan> shared_query_plan = std::move(query_plan);
           auto portal = std::make_pair(statement_type, shared_query_plan);
           _portals.insert(std::make_pair(portal_name, portal));
         } >>
         then >> [=]() { return _connection->send_status_message(NetworkMessageType::BindComplete); };
}

template <typename TConnection, typename TTaskRunner>
boost::future<void> ServerSessionImpl<TConnection, TTaskRunner>::_handle_describe_command(
    const std::string& portal_name) {
  // Ignore this because this message is received in a batch with other commands that handle the response.
  return boost::make_ready_future();
}

template <typename TConnection, typename TTaskRunner>
boost::future<void> ServerSessionImpl<TConnection, TTaskRunner>::_handle_sync_command() {
  if (!_transaction) return boost::make_ready_future();

  _transaction->commit();
  _transaction.reset();

  return boost::make_ready_future();
}

template <typename TConnection, typename TTaskRunner>
boost::future<void> ServerSessionImpl<TConnection, TTaskRunner>::_handle_flush_command() {
  // Ignore this because this message is received in a batch with other commands that handle the response.
  return boost::make_ready_future();
}

template <typename TConnection, typename TTaskRunner>
boost::future<void> ServerSessionImpl<TConnection, TTaskRunner>::_handle_execute_command(
    const std::string& portal_name) {
  auto portal_it = _portals.find(portal_name);
  if (portal_it == _portals.end()) throw std::logic_error("The specified portal does not exist.");

  auto statement_type = portal_it->second.first;
  auto query_plan = portal_it->second.second;

  if (portal_name.empty()) _portals.erase(portal_it);

  if (!_transaction) _transaction = TransactionManager::get().new_transaction_context();

  query_plan->set_transaction_context(_transaction);

  return _task_runner->dispatch_server_task(std::make_shared<ExecuteServerPreparedStatementTask>(query_plan)) >> then >>
         [=](std::shared_ptr<const Table> result_table) {
           // The behavior is a little different compared to SimpleQueryCommand: Send a 'No Data' response
           if (!result_table)
             return _connection->send_status_message(NetworkMessageType::NoDataResponse) >> then >>
                    []() { return uint64_t(0); };

           const auto row_description = QueryResponseBuilder::build_row_description(result_table);
           return _connection->send_row_description(row_description) >> then >> [=]() {
             return QueryResponseBuilder::send_query_response(
                 [=](const std::vector<std::string>& row) { return _connection->send_data_row(row); }, *result_table);
           };
         } >>
         then >> [=](uint64_t row_count) {
           auto complete_message = QueryResponseBuilder::build_command_complete_message(statement_type, row_count);
           return _connection->send_command_complete(complete_message);
         };
}

template class ServerSessionImpl<ClientConnection, TaskRunner>;

}  // namespace opossum
