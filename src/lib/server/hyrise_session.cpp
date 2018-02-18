#include "hyrise_session.hpp"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/asio/placeholders.hpp>
#include <boost/asio/write.hpp>
#include <boost/bind.hpp>

#include <chrono>
#include <iostream>
#include <sql/sql_translator.hpp>
#include <tasks/server/bind_server_prepared_statement.hpp>
#include <tasks/server/execute_server_prepared_statement.hpp>
#include <thread>

#include "SQLParserResult.h"
#include "concurrency/transaction_manager.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/sql_pipeline.hpp"
#include "tasks/server/create_pipeline_task.hpp"
#include "tasks/server/execute_server_query_task.hpp"
#include "tasks/server/load_server_file_task.hpp"
#include "tasks/server/send_query_response_task.hpp"
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
        
        // Something went wrong
        _self.reset();
      }
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
  auto process_request_header = [=](RequestHeader request) {
    // Process the request header

    if (request.message_type == NetworkMessageType::TerminateCommand)
      // Not really unexpected, but this is a way to escape the infinite recursion
      throw std::logic_error("Session was terminated by client");

    // Proceed by reading the packet contents
    std::cout << "reading packet contents" << std::endl;
    return _connection->receive_packet_contents(request.payload_length)
      >> then >> [=] (InputPacket packet_contents) {
      return std::make_pair(request, std::move(packet_contents)); 
    };
  };
  
  auto process_command = [=](std::pair<RequestHeader, InputPacket> packet) {
//    std::cout << "processing packet contents" << std::endl;
//    std::cout << PostgresWireHandler::handle_query_packet(packet.second) << std::endl;
    packet.second.offset = packet.second.data.cbegin();
    switch (packet.first.message_type) {
      case NetworkMessageType::SimpleQueryCommand: {
        const auto sql = PostgresWireHandler::handle_query_packet(packet.second);
        std::cout << "sql" << sql << std::endl;
        return _handle_simple_query_command(sql)
          >> then >> [=]() { _connection->send_ready_for_query(); };
      }
      case NetworkMessageType::ParseCommand:
        // TODO
        // return _accept_parse();
        return boost::make_ready_future();

      case NetworkMessageType::BindCommand:
        // TODO
        // return _accept_bind();
        return boost::make_ready_future();

      case NetworkMessageType::DescribeCommand:
        // TODO
        // return _accept_describe();
        return boost::make_ready_future();

      case NetworkMessageType::SyncCommand:
        // TODO
        // return _accept_sync();
        return boost::make_ready_future();

      case NetworkMessageType::FlushCommand:
        // TODO
        // return _accept_flush();
        return boost::make_ready_future();

      case NetworkMessageType::ExecuteCommand:
        // TODO
        // return _accept_execute();
        return boost::make_ready_future();

      default:
        throw std::logic_error("Unsupported message type");
    }
  };
  
  auto command_result = 
    _connection->receive_packet_header() 
      >> then >> process_request_header
      >> then >> process_command;
  
  return command_result.then(boost::launch::sync, [=] (boost::future<void> result) {
    // Handle any exceptions that have occurred during process_comand
    try {
      result.get();
      return boost::make_ready_future();
    } catch (std::exception& e) {
      return _connection->send_error(e.what())
        >> then >> [=]() { _connection->send_ready_for_query(); };
    }
  }).unwrap()
    // Proceed with the next incoming message
    >> then >> [=]() { _handle_client_requests(); }; 
}

void HyriseSession::_terminate_session() {
  _self.reset();
}

boost::future<void> HyriseSession::_handle_simple_query_command(const std::string sql) {
  using TaskList = std::vector<std::shared_ptr<AbstractTask>>;
  
  auto create_sql_pipeline = [=] () {
    auto task = std::make_shared<CreatePipelineTask>(sql);
    CurrentScheduler::schedule_tasks(TaskList({task}));
    return task->get_future().then(boost::launch::sync, [=] (boost::future<std::shared_ptr<CreatePipelineResult>> f) {
      // This result comes in on the scheduler thread, so we want to dispatch it back to the io_service
      return _io_service.post(boost::asio::use_boost_future)
        // Make sure to be on the main thread before re-throwing the exceptions
        .then(boost::launch::sync, [f = std::move(f)] (boost::future<void>) mutable { return f.get(); });
    }).unwrap();
  };
  
  auto load_table_file = [=](std::string& file_name, std::string& table_name) {
    auto task = std::make_shared<LoadServerFileTask>(file_name, table_name);
    CurrentScheduler::schedule_tasks(TaskList({task}));
    return task->get_future().then(boost::launch::sync, [=] (boost::future<void> f) {
      return _io_service.post(boost::asio::use_boost_future)
        .then(boost::launch::sync, [f = std::move(f)] (boost::future<void>) mutable { f.get(); })
        .then(boost::launch::sync, [=] (boost::future<void>) { /* TODO: Send notice */ });
    }).unwrap();
  };
  
  auto execute_sql_pipeline = [=] (std::shared_ptr<SQLPipeline> sql_pipeline) {
    auto task = std::make_shared<ExecuteServerQueryTask>(sql_pipeline);
    CurrentScheduler::schedule_tasks(TaskList({task}));
    // TODO: This all looks quite terrible
    return task->get_future().then(boost::launch::sync, [=] (boost::future<void> f) {
      return _io_service.post(boost::asio::use_boost_future)
        .then(boost::launch::sync, [f = std::move(f)] (boost::future<void>) mutable { f.get(); })
        .then(boost::launch::sync, [=] (boost::future<void>) { return sql_pipeline; });
    }).unwrap();
  };
  
  auto send_query_response = [=](std::shared_ptr<SQLPipeline> sql_pipeline) {
    auto task = std::make_shared<SendQueryResponseTask>(_connection, sql_pipeline, sql_pipeline->get_result_table());
    CurrentScheduler::schedule_tasks(TaskList({task}));
    return task->get_future() 
      >> then >> [=] () { return _io_service.post(boost::asio::use_boost_future); };
  };
    
  return create_sql_pipeline() >> then >> [=] (std::shared_ptr<CreatePipelineResult> result) {
    return result->is_load_table
      ? load_table_file(result->load_table.value().first, result->load_table.value().second)
      : execute_sql_pipeline(result->sql_pipeline)
          >> then >> send_query_response;
  };
}

void HyriseSession::_accept_parse() {
//  _state = SessionState::ExtendedQuery;
//  _parse_info = std::make_unique<PreparedStatementInfo>(
//      PostgresWireHandler::handle_parse_packet(_input_packet, _expected_input_packet_length));
//  const std::vector<std::shared_ptr<AbstractTask>> tasks = {
//      std::make_shared<CreatePipelineTask>(_parse_info->query)};
//  CurrentScheduler::schedule_tasks(tasks);
}

void HyriseSession::_accept_bind() {
//  auto params = PostgresWireHandler::handle_bind_packet(_input_packet, _expected_input_packet_length);
//
//  const std::vector<std::shared_ptr<ServerTask>> tasks = {
//      std::make_shared<BindServerPreparedStatement>(_self, _sql_pipeline, std::move(params))};
//  CurrentScheduler::schedule_tasks(tasks);
}

void HyriseSession::_accept_execute() {
//  const auto portal = PostgresWireHandler::handle_execute_packet(_input_packet, _expected_input_packet_length);
//
//  const std::vector<std::shared_ptr<ServerTask>> tasks = {std::make_shared<ExecuteServerPreparedStatement>(
//      _self, std::move(_prepared_query_plan), std::move(_prepared_transaction_context))};
//  CurrentScheduler::schedule_tasks(tasks);
}

void HyriseSession::_accept_sync() {
//  PostgresWireHandler::read_values<char>(_input_packet, _expected_input_packet_length);
//  return _async_receive_header();
}

void HyriseSession::_accept_flush() {
//  PostgresWireHandler::read_values<char>(_input_packet, _expected_input_packet_length);
//  return _async_receive_header();
}

void HyriseSession::_accept_describe() {
//  PostgresWireHandler::handle_describe_packet(_input_packet, _expected_input_packet_length);
//  return _async_receive_header();
}

void HyriseSession::pipeline_info(const std::string& notice) {
  auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::Notice);

  PostgresWireHandler::write_value(*output_packet, 'M');
  PostgresWireHandler::write_string(*output_packet, notice);

  // Terminate the notice response
  PostgresWireHandler::write_value(*output_packet, '\0');
//  async_send_packet(*output_packet);
}

void HyriseSession::pipeline_created(std::unique_ptr<SQLPipeline> sql_pipeline) {
//  _sql_pipeline = std::move(sql_pipeline);

//  if (_state == SessionState::ExtendedQuery) {
//    auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::ParseComplete);
//    async_send_packet(*output_packet);
//    return _async_receive_header();
//  }

//  std::vector<std::shared_ptr<AbstractTask>> tasks = {std::make_shared<ExecuteServerQueryTask>(*_sql_pipeline)};
//  CurrentScheduler::schedule_tasks(tasks);
}

void HyriseSession::query_executed() {
//  const std::vector<std::shared_ptr<SendQueryResponseTask>> tasks = {
//      std::make_shared<SendQueryResponseTask>(_connection, *_sql_pipeline, _sql_pipeline->get_result_table())};
//  CurrentScheduler::schedule_tasks(tasks);
}

void HyriseSession::prepared_bound(std::unique_ptr<SQLQueryPlan> query_plan,
                                   std::shared_ptr<TransactionContext> transaction_context) {
  _prepared_query_plan = std::move(query_plan);
  _prepared_transaction_context = std::move(transaction_context);
  auto output_packet = PostgresWireHandler::new_output_packet(NetworkMessageType::BindComplete);
//  async_send_packet(*output_packet);
//  return _async_receive_header();
}

void HyriseSession::prepared_executed(std::shared_ptr<const Table> result_table) {
//  const std::vector<std::shared_ptr<SendQueryResponseTask>> tasks = {
//      std::make_shared<SendQueryResponseTask>(_connection, *_sql_pipeline, std::move(result_table))};
//  CurrentScheduler::schedule_tasks(tasks);
}

void HyriseSession::query_response_sent() { 
//  _send_ready_for_query(); 
}

void HyriseSession::load_table_file(const std::string& file_name, const std::string& table_name) {
//  const std::vector<std::shared_ptr<LoadServerFileTask>> tasks = {
//      std::make_shared<LoadServerFileTask>(file_name, table_name)};
//  CurrentScheduler::schedule_tasks(tasks);
}

void HyriseSession::pipeline_error(const std::string& error_msg) {
//  _send_error(error_msg);
//  _send_ready_for_query();
}

//SessionState HyriseSession::state() const { return _state; }

}  // namespace opossum
