#pragma once

#include <boost/asio/ip/tcp.hpp>

#include "postgres_handler.hpp"

namespace opossum {

using Socket = boost::asio::ip::tcp::socket;

class AbstractOperator;
class TransactionContext;

class Session {
 public:
  explicit Session(Socket socket);
  void start();

 private:
  void _establish_connection();

  void _handle_request();

  void _handle_simple_query();

  void _handle_parse_command();

  void _handle_bind_command();

  void _handle_describe();

  void _handle_execute();
  
  void _sync();

  std::shared_ptr<Socket> _socket;
  PostgresHandler _postgres_handler;
  bool _terminate_session = false;
  std::shared_ptr<TransactionContext> _transaction;
  std::unordered_map<std::string, std::shared_ptr<AbstractOperator>> _portals;
};
}  // namespace opossum
