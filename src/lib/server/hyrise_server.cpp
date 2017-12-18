#include "hyrise_server.hpp"

#include <boost/asio.hpp>

#include "hyrise_session.hpp"

namespace opossum {

HyriseServer::HyriseServer(boost::asio::io_service& io_service, unsigned short port)
    : _acceptor(io_service, tcp::endpoint(tcp::v4(), port)), _socket(io_service) {
  accept_next_connection();
}

void HyriseServer::accept_next_connection() {
  auto start_session = [this](boost::system::error_code error) {
    if (!error) {
      std::make_shared<HyriseSession>(std::move(_socket))->start();
    }
    accept_next_connection();
  };

  _acceptor.async_accept(_socket, start_session);
}

}  // namespace opossum
