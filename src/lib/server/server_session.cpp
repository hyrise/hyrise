#include "server_session.hpp"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/asio/placeholders.hpp>
#include <boost/asio/write.hpp>
#include <boost/bind.hpp>

#include <chrono>
#include <iostream>
#include <thread>

#include "SQLParserResult.h"

#include "sql/sql_pipeline.hpp"
#include "sql/sql_translator.hpp"
#include "tasks/server/bind_server_prepared_statement_task.hpp"
#include "tasks/server/create_pipeline_task.hpp"
#include "tasks/server/execute_server_prepared_statement_task.hpp"
#include "tasks/server/execute_server_query_task.hpp"
#include "tasks/server/load_server_file_task.hpp"
#include "tasks/server/parse_server_prepared_statement_task.hpp"

#include "client_connection.hpp"
#include "hyrise.hpp"
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
  return _connection->receive_startup_packet_header() >> then >> [this](uint32_t startup_packet_length) {
    if (startup_packet_length == 0) {
      // This is a request for SSL, deny it and wait for the next startup packet
      return _connection->send_ssl_denied() >> then >> [this]() { return _perform_session_startup(); };
    }

    return _connection->receive_startup_packet_body(startup_packet_length) >> then >>
           [this]() { return _connection->send_auth(); } >> then >>
           // We need to provide some random server version > 9 here, because some clients require it.
           [this]() { return _connection->send_parameter_status("server_version", "9.5"); } >> then >>
           [this]() { return _connection->send_parameter_status("client_encoding", "UTF8"); } >> then >>
           [this]() { return _connection->send_ready_for_query(); };
  };
}

template <typename TConnection, typename TTaskRunner>
boost::future<void> ServerSessionImpl<TConnection, TTaskRunner>::_handle_client_requests() {
  auto process_command = [this](RequestHeader request) {
    switch (request.message_type) {
      case NetworkMessageType::SimpleQueryCommand: {
        return _connection->receive_simple_query_packet_body(request.payload_length) >> then >>
               [this](std::string sql) { return _handle_simple_query_command(sql); } >> then >>
               [this]() { return _connection->send_ready_for_query(); };
      }

      case NetworkMessageType::ParseCommand: {
        return _connection->receive_parse_packet_body(request.payload_length) >> then >>
               [this](ParsePacket parse_packet) { return _handle_parse_command(parse_packet); };
      }

      case NetworkMessageType::BindCommand: {
        return _connection->receive_bind_packet_body(request.payload_length) >> then >>
               [this](BindPacket bind_packet) { return _handle_bind_command(bind_packet); };
      }

      case NetworkMessageType::DescribeCommand: {
        return _connection->receive_describe_packet_body(request.payload_length) >> then >>
               [this](std::string portal) { return _handle_describe_command(portal); };
      }

      case NetworkMessageType::SyncCommand: {
        return _connection->receive_sync_packet_body(request.payload_length) >> then >>
               [this]() { return _handle_sync_command(); } >> then >>
               [this]() { return _connection->send_ready_for_query(); };
      }

      case NetworkMessageType::FlushCommand: {
        return _connection->receive_flush_packet_body(request.payload_length) >> then >>
               [this]() { return _handle_flush_command(); };
      }

      case NetworkMessageType::ExecuteCommand: {
        return _connection->receive_execute_packet_body(request.payload_length) >> then >>
               [this](std::string portal) { return _handle_execute_command(portal); };
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
  const auto result_table_pair = sql_pipeline->get_result_table();

  Assert(result_table_pair.first == SQLPipelineStatus::Success, "Server cannot handle failed transactions yet");
  const auto result_table = result_table_pair.second;

  auto send_row_data = [this, result_table]() {
    // If there is no result table, e.g. after an INSERT command, we cannot send row data
    if (!result_table) return boost::make_ready_future<uint64_t>(0);

    auto row_description = QueryResponseBuilder::build_row_description(result_table);

    return _connection->send_row_description(row_description) >> then >> [this, result_table]() {
      return QueryResponseBuilder::send_query_response(
          [this, result_table](const std::vector<std::string>& row) { return _connection->send_data_row(row); },
          *result_table);
    };
  };

  auto send_command_complete = [this, sql_pipeline](uint64_t row_count) {
    auto root_op = sql_pipeline->get_physical_plans().front();
    auto complete_message = QueryResponseBuilder::build_command_complete_message(*root_op, row_count);
    return _connection->send_command_complete(complete_message);
  };

  return send_row_data() >> then >> send_command_complete >> then >> [this, sql_pipeline]() {
    auto execution_info = QueryResponseBuilder::build_execution_info_message(sql_pipeline);
    return _connection->send_notice(execution_info);
  };
}

template <typename TConnection, typename TTaskRunner>
boost::future<void> ServerSessionImpl<TConnection, TTaskRunner>::_handle_simple_query_command(const std::string& sql) {
  auto create_sql_pipeline = [this, sql]() {
    return _task_runner->dispatch_server_task(std::make_shared<CreatePipelineTask>(sql, true));
  };

  auto load_table_file = [this](std::string& file_name, std::string& table_name) {
    auto task = std::make_shared<LoadServerFileTask>(file_name, table_name);
    return _task_runner->dispatch_server_task(task) >> then >>
           [this, table_name]() { return _connection->send_notice("Successfully loaded " + table_name); };
  };

  auto execute_sql_pipeline = [this](std::shared_ptr<SQLPipeline> sql_pipeline) {
    auto task = std::make_shared<ExecuteServerQueryTask>(sql_pipeline);
    return _task_runner->dispatch_server_task(task) >> then >> [=]() { return sql_pipeline; };
  };

  // A simple query command invalidates unnamed statements and portals
  if (Hyrise::get().storage_manager.has_prepared_plan("")) Hyrise::get().storage_manager.drop_prepared_plan("");
  _portals.erase("");

  return create_sql_pipeline() >> then >> [this, load_table_file,
                                           execute_sql_pipeline](std::unique_ptr<CreatePipelineResult> result) {
    if (result->load_table) {
      return load_table_file(result->load_table->first, result->load_table->second);
    } else {
      return execute_sql_pipeline(result->sql_pipeline) >> then >>
             [this](std::shared_ptr<SQLPipeline> sql_pipeline) { return _send_simple_query_response(sql_pipeline); };
    }
  };
}

template <typename TConnection, typename TTaskRunner>
boost::future<void> ServerSessionImpl<TConnection, TTaskRunner>::_handle_parse_command(const ParsePacket& parse_info) {
  // Named prepared statements must be explicitly closed before they can be redefined by another Parse message
  // https://www.postgresql.org/docs/10/static/protocol-flow.html
  if (Hyrise::get().storage_manager.has_prepared_plan(parse_info.statement_name)) {
    AssertInput(parse_info.statement_name.empty(),
                "Named prepared statements must be explicitly closed before they can be redefined.");
    Hyrise::get().storage_manager.drop_prepared_plan(parse_info.statement_name);
  }

  auto task = std::make_shared<ParseServerPreparedStatementTask>(parse_info.query);
  return _task_runner->dispatch_server_task(task) >> then >>
         [=](std::unique_ptr<PreparedPlan> prepared_plan) {
           // We know that SQLPipeline is set because the load table command is not allowed in this context
           Hyrise::get().storage_manager.add_prepared_plan(parse_info.statement_name, std::move(prepared_plan));
         } >>
         then >> [this]() { return _connection->send_status_message(NetworkMessageType::ParseComplete); };
}

template <typename TConnection, typename TTaskRunner>
boost::future<void> ServerSessionImpl<TConnection, TTaskRunner>::_handle_bind_command(const BindPacket& packet) {
  // Not using Assert() since it includes file:line info that we don't want to hard code in tests
  AssertInput(Hyrise::get().storage_manager.has_prepared_plan(packet.statement_name),
              "The specified statement does not exist.");

  const auto prepared_plan = Hyrise::get().storage_manager.get_prepared_plan(packet.statement_name);

  if (packet.statement_name.empty()) Hyrise::get().storage_manager.drop_prepared_plan(packet.statement_name);

  auto portal_name = packet.destination_portal;

  // Named portals must be explicitly closed before they can be redefined by another Bind message,
  // but this is not required for the unnamed portal.
  // https://www.postgresql.org/docs/10/static/protocol-flow.html
  auto portal_it = _portals.find(portal_name);
  if (portal_it != _portals.end()) {
    // Not using Assert() since it includes file:line info that we don't want to hard code in tests
    AssertInput(portal_name.empty(), "Named portals must be explicitly closed before they can be redefined.");
    _portals.erase(portal_it);
  }

  auto task = std::make_shared<BindServerPreparedStatementTask>(prepared_plan, packet.params);
  return _task_runner->dispatch_server_task(task) >> then >>
         [this, portal_name](std::shared_ptr<AbstractOperator> physical_plan) {
           _portals.emplace(portal_name, physical_plan);
         } >>
         then >> [this]() { return _connection->send_status_message(NetworkMessageType::BindComplete); };
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
  Assert(portal_it != _portals.end(), "The specified portal does not exist.");

  const auto physical_plan = portal_it->second;

  if (portal_name.empty()) _portals.erase(portal_it);

  if (!_transaction) _transaction = Hyrise::get().transaction_manager.new_transaction_context();

  physical_plan->set_transaction_context_recursively(_transaction);

  return _task_runner->dispatch_server_task(std::make_shared<ExecuteServerPreparedStatementTask>(physical_plan)) >>
         then >>
         [this](std::shared_ptr<const Table> result_table) {
           // The behavior is a little different compared to SimpleQueryCommand: Send a 'No Data' response
           if (!result_table) {
             return _connection->send_status_message(NetworkMessageType::NoDataResponse) >> then >>
                    []() { return uint64_t(0); };
           }

           const auto row_description = QueryResponseBuilder::build_row_description(result_table);
           return _connection->send_row_description(row_description) >> then >> [this, result_table]() {
             return QueryResponseBuilder::send_query_response(
                 [this](const std::vector<std::string>& row) { return _connection->send_data_row(row); },
                 *result_table);
           };
         } >>
         then >> [this, physical_plan](uint64_t row_count) {
           auto complete_message = QueryResponseBuilder::build_command_complete_message(*physical_plan, row_count);
           return _connection->send_command_complete(complete_message);
         };
}

template class ServerSessionImpl<ClientConnection, TaskRunner>;

}  // namespace opossum
