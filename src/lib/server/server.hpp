#pragma once

#include <atomic>

#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>

#include "server_session.hpp"

namespace opossum {

class Server {
 public:
  Server(boost::asio::io_service& io_service, uint16_t port);

  uint16_t get_port_number();

  void toggle_dont_accept_next_connection();

 protected:
  void _accept_next_connection();
  void _start_session(boost::system::error_code error);

  boost::asio::io_service& _io_service;
  boost::asio::ip::tcp::acceptor _acceptor;
  boost::asio::ip::tcp::socket _socket;

  std::atomic<bool> _dont_accept_next_connection = false;

  std::shared_ptr<ServerSession> _server_session;
};

}  // namespace opossum
