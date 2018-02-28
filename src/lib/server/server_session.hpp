#pragma once

#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/thread/future.hpp>

#include <memory>
#include <optional>

#include "postgres_wire_handler.hpp"
#include "server/client_connection.hpp"
#include "sql/sql_pipeline.hpp"
#include "tasks/server/server_task.hpp"
#include "types.hpp"

namespace opossum {

using boost::asio::ip::tcp;

template <typename T>
class ServerSessionImpl {
 public:
  explicit ServerSessionImpl(boost::asio::io_service& io_service, std::shared_ptr<T> connection)
      : _io_service(io_service), _connection(connection) {}

  boost::future<void> start();

 protected:
  boost::future<void> _perform_session_startup();

  boost::future<void> _handle_client_requests();
  boost::future<void> _handle_simple_query_command(const std::string sql);
  boost::future<void> _handle_parse_command(std::unique_ptr<ParsePacket> parse_info);
  boost::future<void> _handle_bind_command(BindPacket packet);
  boost::future<void> _handle_describe_command(std::string portal_name);
  boost::future<void> _handle_execute_command(std::string portal_name);
  boost::future<void> _handle_sync_command();
  boost::future<void> _handle_flush_command();

  template <typename TResult>
  auto _dispatch_server_task(std::shared_ptr<TResult> task) -> decltype(task->get_future());

  boost::asio::io_service& _io_service;
  std::shared_ptr<T> _connection;

  std::shared_ptr<TransactionContext> _transaction;
  std::unordered_map<std::string, std::shared_ptr<SQLPipeline>> _prepared_statements;
  // TODO(lawben): The type of _portals will change when prepared statements are supported in the SQLPipeline
  std::unordered_map<std::string, std::pair<hsql::StatementType, std::shared_ptr<SQLQueryPlan>>> _portals;
};

// The corresponding template instantiation takes place in the .cpp
using ServerSession = ServerSessionImpl<ClientConnection>;

}  // namespace opossum
