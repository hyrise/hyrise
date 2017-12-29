#pragma once

#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>

namespace opossum {

using boost::asio::ip::tcp;

class HyriseServer {
 public:
  HyriseServer(boost::asio::io_service& io_service, uint16_t port);

 protected:
  void accept_next_connection();

  boost::asio::io_service& _io_service;
  tcp::acceptor _acceptor;
  tcp::socket _socket;
};

}  // namespace opossum
