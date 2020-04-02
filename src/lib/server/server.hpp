#pragma once

#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>

#include "server_types.hpp"
#include "session.hpp"

namespace opossum {

/* In the following a short description of the classes used for the server implementation.

*  Server - Opens and binds a server socket. Starts a new session per client.
*  Session - Creates a data socket for client server communication. It is responsible for the message flow and holds
*            session-specific data.
*  PostgresProtocolHandler - This class operates on the message level. It serializes and de-serializes information from
*                            messages.
*  PostgresMessageTypes - Set of different message types supported by Hyrise.
*  ReadBuffer - Dedicated ring buffer for reading information from the network socket. Also does network to host byte
*               conversion for integer types.
*  WriteBuffer - Dedicated ring buffer for writing information to the network socket. Does host to network byte
*                conversion for integer types.
*  RingBufferIterator - Implements ring buffer logic used by the two buffers.
*  QueryHandler - Interface between the server and the database logic. Operations, such as creating an SQLPipeline or
*                 PQP are handled in this class.
*  ResultSerializer - Converts query result information (such as ColumnTypes, but also the data itself) to appropriate
*                     PostgreSQL protocol types and writes them to the network socket.
*/

class Server {
 public:
  Server(const boost::asio::ip::address& address, const uint16_t port, const SendExecutionInfo send_execution_info);

  // Start server to accept new sessions.
  void run();

  // Return the port the server is running on.
  uint16_t server_port() const;

  // Get the current address the server is running. This is important especially for multi-NIC devices.
  boost::asio::ip::address server_address() const;

  // Shutdown Hyrise server.
  void shutdown();

 private:
  void _accept_new_session();

  void _start_session(const std::shared_ptr<Session>& new_session, const boost::system::error_code& error);

  boost::asio::io_service _io_service;
  boost::asio::ip::tcp::acceptor _acceptor;
  const SendExecutionInfo _send_execution_info;
};
}  // namespace opossum
