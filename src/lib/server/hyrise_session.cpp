#include "hyrise_session.hpp"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/asio/placeholders.hpp>
#include <boost/asio/write.hpp>
#include <boost/bind.hpp>

#include <chrono>
#include <iostream>
#include <sql/sql_translator.hpp>
#include <tasks/server/bind_server_prepared_statement_task.hpp>
#include <tasks/server/execute_server_prepared_statement_task.hpp>
#include <thread>

#include "SQLParserResult.h"
#include "concurrency/transaction_manager.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/sql_pipeline.hpp"
#include "tasks/server/create_pipeline_task.hpp"
#include "tasks/server/execute_server_query_task.hpp"
#include "tasks/server/load_server_file_task.hpp"
#include "tasks/server/send_query_response_task.hpp"
#include "tasks/server/commit_transaction_task.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/load_table.hpp"
#include "client_connection.hpp"
#include "then_operator.hpp"
#include "use_boost_future.hpp"

namespace opossum {

using opossum::then_operator::then;

void HyriseSession::start() {
  // Keep a pointer to itself that will be released once the connection is closed
  _self = shared_from_this();
  
  (_perform_session_startup() >> then >> [=] () { return _handle_client_requests(); })
    .then(boost::launch::sync, [=](boost::future<void> f) {
      try {
        f.get();
      } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
      }

      // Release self and close the socket
      _self.reset();
    }
  );
}

boost::future<void> HyriseSession::_perform_session_startup() {
  return _connection->receive_startup_packet_header() >> then >> [=](uint32_t startup_packet_length) {
    if (startup_packet_length == 0) {
      // This is a request for SSL, deny it and wait for the next startup packet
      return _connection->send_ssl_denied()
        >> then >> [=]() { return _perform_session_startup(); };
    }

    return _connection->receive_startup_packet_contents(startup_packet_length)
      >> then >> [=]() { return _connection->send_auth(); }
      >> then >> [=]() { return _connection->send_ready_for_query(); };
  };
}

boost::future<void> HyriseSession::_handle_client_requests() {
  auto receive_packet_contents = [=](RequestHeader request) {
    return _connection->receive_packet_contents(request.payload_length)
      >> then >> [=] (InputPacket packet_contents) {
      return std::make_pair(request, std::move(packet_contents)); 
    };
  };
  
  auto process_command = [=](std::pair<RequestHeader, InputPacket> packet) {
    switch (packet.first.message_type) {
      case NetworkMessageType::SimpleQueryCommand: {
        const auto sql = PostgresWireHandler::handle_query_packet(packet.second);
        return _handle_simple_query_command(sql)
          >> then >> [=]() { _connection->send_ready_for_query(); };
      }
      case NetworkMessageType::ParseCommand: {
        auto parse_info = std::make_unique<ParsePacket>(
          PostgresWireHandler::handle_parse_packet(packet.second));
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
        // Packet does not contain any contents
        return _handle_sync_command()
          >> then >> [=]() { _connection->send_ready_for_query(); };
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
    if (request.message_type == NetworkMessageType::TerminateCommand)
      return boost::make_ready_future();

    auto command_result = receive_packet_contents(request) >> then >> process_command;

    // Handle any exceptions that have occurred during process_command
    return command_result.then(boost::launch::sync, [=] (boost::future<void> result) {
      try {
        result.get();
        return boost::make_ready_future();
      } catch (std::exception& e) {
        return _connection->send_error(e.what())
          >> then >> [=]() { _connection->send_ready_for_query(); };
      }
    }).unwrap()
      // Proceed with the next incoming message
      >> then >> boost::bind(&HyriseSession::_handle_client_requests, this);
  };
}

boost::future<void> HyriseSession::_handle_simple_query_command(const std::string sql) {
  auto create_sql_pipeline = [=] () {
    return _dispatch_server_task(std::make_shared<CreatePipelineTask>(sql, true));
  };
  
  auto load_table_file = [=](std::string& file_name, std::string& table_name) {
    auto task = std::make_shared<LoadServerFileTask>(file_name, table_name);
    return _dispatch_server_task(task)
      >> then >> [=] () { return _connection->send_notice("Successfully loaded " + table_name); };
  };
  
  auto execute_sql_pipeline = [=] (std::shared_ptr<SQLPipeline> sql_pipeline) {
    auto task = std::make_shared<ExecuteServerQueryTask>(sql_pipeline);
    return _dispatch_server_task(task)
      >> then >> [=] () { return sql_pipeline; };
  };
  
  auto send_query_response = [=](std::shared_ptr<SQLPipeline> sql_pipeline) {
    auto statement_type = sql_pipeline->get_parsed_sql_statements().front()->getStatements().front()->type();
    auto task = std::make_shared<SendQueryResponseTask>(
      _connection, statement_type, sql_pipeline, sql_pipeline->get_result_table());
    return _dispatch_server_task(task);
  };
  
  // A simple query command invalidates unnamed statements and portals
  _prepared_statements.erase("");
  _portals.erase("");
    
  return create_sql_pipeline() >> then >> [=] (std::shared_ptr<CreatePipelineResult> result) {
    return result->is_load_table
      ? load_table_file(result->load_table.value().first, result->load_table.value().second)
      : execute_sql_pipeline(result->sql_pipeline)
          >> then >> send_query_response;
  };
}

boost::future<void> HyriseSession::_handle_parse_command(std::unique_ptr<ParsePacket> parse_info) {
  auto prepared_statement_name = parse_info->statement_name;

  // Named prepared statements must be explicitly closed before they can be redefined by another Parse message
  // https://www.postgresql.org/docs/10/static/protocol-flow.html
  auto statement_it = _prepared_statements.find(prepared_statement_name);
  if (statement_it != _prepared_statements.end()) {
    if (!prepared_statement_name.empty())
      throw std::logic_error("Named prepared statements must be explicitly closed before they can be redefined");
    _prepared_statements.erase(statement_it);
  }
  
  return _dispatch_server_task(std::make_shared<CreatePipelineTask>(parse_info->query))
    >> then >> [=] (std::shared_ptr<CreatePipelineResult> result) {
      // We know that SQL Pipeline is set in the result because the load table command is not allowed in this context
      _prepared_statements.insert(std::make_pair(prepared_statement_name, result->sql_pipeline));
    } >> then >> [=] () { return _connection->send_status_message(NetworkMessageType::ParseComplete); };
}

boost::future<void> HyriseSession::_handle_bind_command(BindPacket packet) {
  auto statement_it = _prepared_statements.find(packet.statement_name);
  if (statement_it == _prepared_statements.end())
    throw std::logic_error("Unknown statement");

  auto sql_pipeline = statement_it->second;
  if (packet.statement_name.empty())
    _prepared_statements.erase(statement_it);
  
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
  return _dispatch_server_task(task)
    >> then >> [=] (std::unique_ptr<SQLQueryPlan> query_plan) {
      std::shared_ptr<SQLQueryPlan> shared_query_plan = std::move(query_plan);
      auto portal = std::make_pair(statement_type, shared_query_plan);
      _portals.insert(std::make_pair(portal_name, portal));
    } >> then >> [=] () { return _connection->send_status_message(NetworkMessageType::BindComplete); };
}

boost::future<void> HyriseSession::_handle_describe_command(std::string portal_name) {
  // Ignore this for now
  return boost::make_ready_future();
}

boost::future<void> HyriseSession::_handle_sync_command() {
  return _dispatch_server_task(std::make_shared<CommitTransactionTask>(_transaction));
}

boost::future<void> HyriseSession::_handle_flush_command() {
  // Ignore this for now
  return boost::make_ready_future();
}

boost::future<void> HyriseSession::_handle_execute_command(std::string portal_name) {
  auto portal_it = _portals.find(portal_name);
  if (portal_it == _portals.end())
    throw std::logic_error("Unknown portal");
  
  auto statement_type = portal_it->second.first;
  auto query_plan = portal_it->second.second;
  // TODO: Is it even possible to execute a query plan multiple times?
  if (portal_name.empty())
    _portals.erase(portal_it);
  
  if (!_transaction)
    _transaction = TransactionManager::get().new_transaction_context();

  query_plan->set_transaction_context(_transaction);
  
  return _dispatch_server_task(std::make_shared<ExecuteServerPreparedStatementTask>(query_plan, _transaction))
    >> then >> [=] (std::shared_ptr<const Table> result_table) {
      auto task = std::make_shared<SendQueryResponseTask>(_connection, statement_type, nullptr, result_table);
      return _dispatch_server_task(task);
    };
}

template<typename T>
auto HyriseSession::_dispatch_server_task(std::shared_ptr<T> task) -> decltype(task->get_future()) {
  using TaskList = std::vector<std::shared_ptr<AbstractTask>>;

  CurrentScheduler::schedule_tasks(TaskList({task}));

  return task->get_future().then(boost::launch::sync, [=] (boost::future<typename T::result_type> result) {
    // This result comes in on the scheduler thread, so we want to dispatch it back to the io_service
    return _io_service.post(boost::asio::use_boost_future)
      // Make sure to be on the main thread before re-throwing the exceptions
      >> then >> [result = std::move(result)] () mutable { 
//        try {
//          throw std::logic_error("just a test");
      return result.get();
//          return result.get();
//        } catch (std::exception& e) {
//          throw e;
//        }
      };
  }).unwrap();
}

}  // namespace opossum
