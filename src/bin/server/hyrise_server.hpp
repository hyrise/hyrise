#pragma once

namespace opossum {

using boost::asio::ip::tcp;

class HyriseServer {
 public:
  HyriseServer(boost::asio::io_service& io_service, unsigned short port);

 protected:
  void accept_next_connection();

  tcp::acceptor _acceptor;
  tcp::socket _socket;
};

}  // namespace opossum