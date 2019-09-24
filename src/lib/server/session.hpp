#pragma once

#include "concurrency/transaction_context.hpp"
#include "postgres_protocol_handler.hpp"

#include "operators/abstract_operator.hpp"
#include "scheduler/operator_task.hpp"

namespace opossum {

// The session class implements the communication flow and stores session specific information such as portals.
class Session {
 public:
  // explicit Session(Socket socket);
  explicit Session(boost::asio::io_service& io_service);

  // Start new session.
  void start();

  std::shared_ptr<Socket> get_socket();

 private:
  // Establish new connection by exchanging parameters.
  void _establish_connection();

  // Determine message and call the appropiate method.
  void _handle_request();

  // Execute plain SQL statement.
  void _handle_simple_query();

  // Parse prepared statement.
  void _handle_parse_command();

  // Bind prepared statement.
  void _handle_bind_command();

  // Send parameter and row description.
  void _handle_describe();

  // Execute prepared statement.
  void _handle_execute();

  // Commit current transaction.
  void _sync();



  const std::shared_ptr<Socket> _socket;
  const std::shared_ptr<PostgresProtocolHandler> _postgres_protocol_handler;
  bool _terminate_session = false;
  std::shared_ptr<TransactionContext> _transaction;
  std::unordered_map<std::string, std::shared_ptr<AbstractOperator>> _portals;
};
}  // namespace opossum
