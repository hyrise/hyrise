#pragma once

#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>

#include "session.hpp"

namespace opossum {

// The server class is responsible for setting up and listening to the system port.
class Server {
 public:
  explicit Server(const uint16_t port);

  // Return the port the server is running on.
  uint16_t get_port() const;

  // Shutdown Hyrise server.
  void shutdown();

  // Start server by running boost io_service.
  void run();

 private:
  void _accept_new_session();

  void _start_session(const boost::system::error_code& error);

  boost::asio::io_service _io_service;
  boost::asio::ip::tcp::socket _socket;
  boost::asio::ip::tcp::acceptor _acceptor;
};
}  // namespace opossum
