#include "server_session.hpp"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/asio/placeholders.hpp>
#include <boost/asio/write.hpp>
#include <boost/bind.hpp>

#include <chrono>
#include <iostream>
#include <thread>

#include "SQLParserResult.h"
#include "client_connection.hpp"
#include "concurrency/transaction_manager.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_translator.hpp"
#include "tasks/server/bind_server_prepared_statement_task.hpp"
#include "tasks/server/commit_transaction_task.hpp"
#include "tasks/server/create_pipeline_task.hpp"
#include "tasks/server/execute_server_prepared_statement_task.hpp"
#include "tasks/server/execute_server_query_task.hpp"
#include "tasks/server/load_server_file_task.hpp"
#include "tasks/server/send_query_response_task.hpp"
#include "then_operator.hpp"
#include "types.hpp"
#include "use_boost_future.hpp"
#include "utils/assert.hpp"
#include "utils/load_table.hpp"

namespace opossum {

using opossum::then_operator::then;

void ServerSession::start() {
  // Keep a pointer to itself that will be released once the connection is closed
  _self = shared_from_this();

  (_perform_session_startup() >> then >> [=]() { return _handle_client_requests(); })
      // Use .then instead of >> then >> to be able to handle exceptions
      .then(boost::launch::sync, [=](boost::future<void> f) {
        try {
          f.get();
        } catch (std::exception& e) {
          std::cerr << e.what() << std::endl;
        }

        // Release self and close the socket
        _self.reset();
      });
}

boost::future<void> ServerSession::_perform_session_startup() {
  return _connection->receive_startup_packet_header() >> then >> [=](uint32_t startup_packet_length) {
    if (startup_packet_length == 0) {
      // This is a request for SSL, deny it and wait for the next startup packet
      return _connection->send_ssl_denied() >> then >> [=]() { return _perform_session_startup(); };
    }

    return _connection->receive_startup_packet_contents(startup_packet_length) >> then >>
           [=]() { return _connection->send_auth(); } >> then >> [=]() { return _connection->send_ready_for_query(); };
  };
}

boost::future<void> ServerSession::_handle_client_requests() {
  auto receive_packet_contents = [=](RequestHeader request) {
    return _connection->receive_packet_contents(request.payload_length) >> then >>
           [=](InputPacket packet_contents) { return std::make_pair(request, std::move(packet_contents)); };
  };

  auto process_command = [=](std::pair<RequestHeader, InputPacket> packet) {
    switch (packet.first.message_type) {
      case NetworkMessageType::SimpleQueryCommand: {
        const auto sql = PostgresWireHandler::handle_query_packet(packet.second);
        return _handle_simple_query_command(sql) >> then >> [=]() { _connection->send_ready_for_query(); };
      }
      case NetworkMessageType::ParseCommand: {
        auto parse_info = std::make_unique<ParsePacket>(PostgresWireHandler::handle_parse_packet(packet.second));
        return _handle_parse_command(std::move(parse_info));
      }

      case NetworkMessageType::BindCommand: {
        auto bind_packet = PostgresWireHandler::handle_bind_packet(packet.second);
        return _handle_bind_command(std::move(bind_packet));
      }

      case NetworkMessageType::DescribeCommand: {
        auto portal = PostgresWireHandler::handle_describe_packet(packet.second);
        return _handle_describe_command(portal);
      }

      case NetworkMessageType::SyncCommand: {
        // Packet has no content
        return _handle_sync_command() >> then >> [=]() { _connection->send_ready_for_query(); };
      }

      case NetworkMessageType::FlushCommand: {
        return _handle_flush_command();
      }

      case NetworkMessageType::ExecuteCommand: {
        auto portal = PostgresWireHandler::handle_execute_packet(packet.second);
        return _handle_execute_command(std::move(portal));
      }

      default:
        throw std::logic_error("Unsupported message type");
    }
  };

  return _connection->receive_packet_header() >> then >> [=](RequestHeader request) {
    if (request.message_type == NetworkMessageType::TerminateCommand) return boost::make_ready_future();

    auto command_result = receive_packet_contents(request) >> then >> process_command;

    // Handle any exceptions that have occurred during process_command. For this, we need to call .then() explicitly,
    // because >> then >> does not handle execptions
    return command_result
               .then(boost::launch::sync,
                     [=](boost::future<void> result) {
                       try {
                         result.get();
                         return boost::make_ready_future();
                       } catch (std::exception& e) {
                         // Abort the current transaction
                         if (_transaction) {
                           _transaction->rollback();
                           _transaction.reset();
                         }

                         return _connection->send_error(e.what()) >> then >>
                                [=]() { _connection->send_ready_for_query(); };
                       }
                     })
               .unwrap()
           // Proceed with the next incoming message
           >> then >> boost::bind(&ServerSession::_handle_client_requests, this);
  };
}

boost::future<void> ServerSession::_handle_simple_query_command(const std::string sql) {
  auto create_sql_pipeline = [=]() { return _dispatch_server_task(std::make_shared<CreatePipelineTask>(sql, true)); };

  auto load_table_file = [=](std::string& file_name, std::string& table_name) {
    auto task = std::make_shared<LoadServerFileTask>(file_name, table_name);
    return _dispatch_server_task(task) >> then >>
           [=]() { return _connection->send_notice("Successfully loaded " + table_name); };
  };

  auto execute_sql_pipeline = [=](std::shared_ptr<SQLPipeline> sql_pipeline) {
    auto task = std::make_shared<ExecuteServerQueryTask>(sql_pipeline);
    return _dispatch_server_task(task) >> then >> [=]() { return sql_pipeline; };
  };

  auto send_query_response = [=](std::shared_ptr<SQLPipeline> sql_pipeline) {
    auto result_table = sql_pipeline->get_result_table();

    auto send_row_data = [=]() {
      // If there is no result table, e.g. after an INSERT command, we cannot send row data
      if (!result_table) return boost::make_ready_future<uint64_t>(0);

      auto row_description = SendQueryResponseTask::build_row_description(sql_pipeline->get_result_table());

      auto task = std::make_shared<SendQueryResponseTask>(_connection, result_table);
      return _connection->send_row_description(row_description) >> then >>
             [=]() { return _dispatch_server_task(task); };
    };

    return send_row_data() >> then >>
           [=](uint64_t row_count) {
             auto statement_type = sql_pipeline->get_parsed_sql_statements().front()->getStatements().front()->type();
             auto complete_message = SendQueryResponseTask::build_command_complete_message(statement_type, row_count);
             return _connection->send_command_complete(complete_message);
           } >>
           then >> [=]() {
             auto execution_info = SendQueryResponseTask::build_execution_info_message(sql_pipeline);
             return _connection->send_notice(execution_info);
           };
  };

  // A simple query command invalidates unnamed statements and portals
  _prepared_statements.erase("");
  _portals.erase("");

  return create_sql_pipeline() >> then >> [=](std::unique_ptr<CreatePipelineResult> result) {
    if (result->load_table.has_value()) {
      return load_table_file(result->load_table.value().first, result->load_table.value().second);
    } else {
      return execute_sql_pipeline(result->sql_pipeline) >> then >> send_query_response;
    }
  };
}

boost::future<void> ServerSession::_handle_parse_command(std::unique_ptr<ParsePacket> parse_info) {
  auto prepared_statement_name = parse_info->statement_name;

  // Named prepared statements must be explicitly closed before they can be redefined by another Parse message
  // https://www.postgresql.org/docs/10/static/protocol-flow.html
  auto statement_it = _prepared_statements.find(prepared_statement_name);
  if (statement_it != _prepared_statements.end()) {
    if (!prepared_statement_name.empty()) {
      throw std::logic_error("Named prepared statements must be explicitly closed before they can be redefined");
    }
    _prepared_statements.erase(statement_it);
  }

  return _dispatch_server_task(std::make_shared<CreatePipelineTask>(parse_info->query)) >> then >>
         [=](std::unique_ptr<CreatePipelineResult> result) {
           // We know that SQLPipeline is set because the load table command is not allowed in this context
           _prepared_statements.insert(std::make_pair(prepared_statement_name, result->sql_pipeline));
         } >>
         then >> [=]() { return _connection->send_status_message(NetworkMessageType::ParseComplete); };
}

boost::future<void> ServerSession::_handle_bind_command(BindPacket packet) {
  auto statement_it = _prepared_statements.find(packet.statement_name);
  if (statement_it == _prepared_statements.end()) throw std::logic_error("Unknown statement");

  auto sql_pipeline = statement_it->second;
  if (packet.statement_name.empty()) _prepared_statements.erase(statement_it);

  auto portal_name = packet.destination_portal;

  // Named portals must be explicitly closed before they can be redefined by another Bind message,
  // but this is not required for the unnamed portal.
  // https://www.postgresql.org/docs/10/static/protocol-flow.html
  auto portal_it = _portals.find(portal_name);
  if (portal_it != _portals.end()) {
    if (!portal_name.empty())
      throw std::logic_error("Named portals must be explicitly closed before they can be redefined");
    _portals.erase(portal_it);
  }

  auto statement_type = sql_pipeline->get_parsed_sql_statements().front()->getStatements().front()->type();

  auto task = std::make_shared<BindServerPreparedStatementTask>(sql_pipeline, std::move(packet.params));
  return _dispatch_server_task(task) >> then >>
         [=](std::unique_ptr<SQLQueryPlan> query_plan) {
           std::shared_ptr<SQLQueryPlan> shared_query_plan = std::move(query_plan);
           auto portal = std::make_pair(statement_type, shared_query_plan);
           _portals.insert(std::make_pair(portal_name, portal));
         } >>
         then >> [=]() { return _connection->send_status_message(NetworkMessageType::BindComplete); };
}

boost::future<void> ServerSession::_handle_describe_command(std::string portal_name) {
  // Ignore this because this message is received in a batch with other commands that handle the response.
  return boost::make_ready_future();
}

boost::future<void> ServerSession::_handle_sync_command() {
  if (!_transaction) return boost::make_ready_future();

  return _dispatch_server_task(std::make_shared<CommitTransactionTask>(_transaction)) >> then >>
         [=]() { _transaction.reset(); };
}

boost::future<void> ServerSession::_handle_flush_command() {
  // Ignore this because this message is received in a batch with other commands that handle the response.
  return boost::make_ready_future();
}

boost::future<void> ServerSession::_handle_execute_command(std::string portal_name) {
  auto portal_it = _portals.find(portal_name);
  if (portal_it == _portals.end()) throw std::logic_error("Unknown portal");

  auto statement_type = portal_it->second.first;
  auto query_plan = portal_it->second.second;

  if (portal_name.empty()) _portals.erase(portal_it);

  if (!_transaction) _transaction = TransactionManager::get().new_transaction_context();

  query_plan->set_transaction_context(_transaction);

  return _dispatch_server_task(std::make_shared<ExecuteServerPreparedStatementTask>(query_plan)) >> then >>
         [=](std::shared_ptr<const Table> result_table) {
           // The behavior is a little different compared to SimpleQueryCommand: Send a 'No Data' response
           if (!result_table)
             return _connection->send_status_message(NetworkMessageType::NoDataResponse) >> then >>
                    []() { return uint64_t(0); };

           auto task = std::make_shared<SendQueryResponseTask>(_connection, result_table);
           return _dispatch_server_task(task);
         } >>
         then >> [=](uint64_t row_count) {
           auto complete_message = SendQueryResponseTask::build_command_complete_message(statement_type, row_count);
           return _connection->send_command_complete(complete_message);
         };
}

template <typename T>
auto ServerSession::_dispatch_server_task(std::shared_ptr<T> task) -> decltype(task->get_future()) {
  using TaskList = std::vector<std::shared_ptr<AbstractTask>>;

  CurrentScheduler::schedule_tasks(TaskList({task}));

  return task->get_future()
      .then(boost::launch::sync,
            [=](boost::future<typename T::result_type> result) {
              // This result comes in on the scheduler thread, so we want to dispatch it back to the io_service
              return _io_service.post(boost::asio::use_boost_future)
                     // Make sure to be on the main thread before re-throwing the exceptions
                     >> then >> [result = std::move(result)]() mutable {
                return result.get();
              };
            })
      .unwrap();
}

}  // namespace opossum
