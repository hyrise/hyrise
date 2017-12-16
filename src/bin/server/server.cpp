// Files in the /bin folder are not tested. Everything that can be tested should be in the /lib folder and this file
// should be as short as possible.

#include <cstdlib>
#include <iostream>
#include <memory>
#include <utility>

#include <boost/asio.hpp>

#include "session.hpp"

namespace opossum {

class server {
 public:
  server(boost::asio::io_service& io_service, unsigned short port)
      : _acceptor(io_service, tcp::endpoint(tcp::v4(), port)), _socket(io_service) {
    do_accept();
  }

 private:
  void do_accept() {
    auto start_session = [this](boost::system::error_code ec) {
      if (!ec) {
        std::make_shared<Session>(std::move(_socket))->start();
      }
      do_accept();
    };

    _acceptor.async_accept(_socket, start_session);
  }

  tcp::acceptor _acceptor;
  tcp::socket _socket;
};

}  // namespace opossum

int main(int argc, char* argv[]) {
  try {
    if (argc != 2) {
      std::cerr << "Usage: " << argv[0] << " <port>\n";
      return 1;
    }

    boost::asio::io_service io_service;

    opossum::server s(io_service, std::atoi(argv[1]));

    io_service.run();
  } catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}
