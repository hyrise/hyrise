#pragma once

#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>

#include "session.hpp"

namespace opossum {

class Server {
 public:
  explicit Server(const uint16_t port);

  // Return port
  uint16_t get_port() const;

  void shutdown();

  // Start boost io_service. This call is blocking.
  void run();

 private:
  void _accept_new_session();

  void _start_session();

  boost::asio::io_service _io_service;
  boost::asio::ip::tcp::socket _socket;
  boost::asio::ip::tcp::acceptor _acceptor;
};
}  // namespace opossum
