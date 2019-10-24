#pragma once

#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>

#include "session.hpp"

namespace opossum {

// The server class is responsible for setting up and listening to the system port.
class Server {
 public:
  Server(const boost::asio::ip::address& address, const uint16_t port, const SendExecutionInfo send_execution_info);

  // Start server by running boost io_service.
  void run();

  // Return the port the server is running on.
  uint16_t get_port() const;

  // Get the current address the server is running. This is important especially for multi-NIC devices.
  boost::asio::ip::address get_address() const;

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
