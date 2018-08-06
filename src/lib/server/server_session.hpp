#pragma once

#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/thread/future.hpp>

#include <memory>

#include "client_connection.hpp"
#include "postgres_wire_handler.hpp"
#include "sql/sql_pipeline.hpp"
#include "task_runner.hpp"
#include "types.hpp"

namespace opossum {

template <typename TConnection, typename TTaskRunner>
class ServerSessionImpl : public std::enable_shared_from_this<ServerSessionImpl<TConnection, TTaskRunner>> {
 public:
  explicit ServerSessionImpl(std::shared_ptr<TConnection> connection, std::shared_ptr<TTaskRunner> task_runner)
      : _connection(connection), _task_runner(task_runner) {}

  boost::future<void> start();

 protected:
  boost::future<void> _perform_session_startup();

  boost::future<void> _handle_client_requests();
  boost::future<void> _handle_simple_query_command(const std::string& sql);
  boost::future<void> _handle_parse_command(const ParsePacket& parse_info);
  boost::future<void> _handle_bind_command(const BindPacket& packet);
  boost::future<void> _handle_describe_command(const std::string& portal_name);
  boost::future<void> _handle_execute_command(const std::string& portal_name);
  boost::future<void> _handle_sync_command();
  boost::future<void> _handle_flush_command();

  boost::future<void> _send_simple_query_response(const std::shared_ptr<SQLPipeline>& sql_pipeline);

  std::shared_ptr<TConnection> _connection;
  std::shared_ptr<TTaskRunner> _task_runner;

  std::shared_ptr<TransactionContext> _transaction;
  std::unordered_map<std::string, std::shared_ptr<SQLPipeline>> _prepared_statements;
  // TODO(lawben): The type of _portals will change when prepared statements are supported in the SQLPipeline
  std::unordered_map<std::string, std::pair<hsql::StatementType, std::shared_ptr<SQLQueryPlan>>> _portals;
};

// The corresponding template instantiation takes place in the .cpp
using ServerSession = ServerSessionImpl<ClientConnection, TaskRunner>;

}  // namespace opossum
