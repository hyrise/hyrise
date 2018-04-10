#pragma once

#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>

namespace opossum {

class Server {
 public:
  Server(boost::asio::io_service& io_service, uint16_t port);

 protected:
  void accept_next_connection();
  void start_session(boost::system::error_code error);

  boost::asio::io_service& _io_service;
  boost::asio::ip::tcp::acceptor _acceptor;
  boost::asio::ip::tcp::socket _socket;
};

}  // namespace opossum
