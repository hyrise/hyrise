#include "hyrise_server.hpp"

#include <boost/asio/placeholders.hpp>
#include <boost/bind.hpp>

#include "hyrise_session.hpp"
#include "client_connection.hpp"

namespace opossum {

HyriseServer::HyriseServer(boost::asio::io_service& io_service, uint16_t port)
    : _io_service(io_service), _acceptor(io_service, tcp::endpoint(tcp::v4(), port)), _socket(io_service) {
  accept_next_connection();
}

void HyriseServer::accept_next_connection() {
  _acceptor.async_accept(_socket, boost::bind(&HyriseServer::start_session, this, boost::asio::placeholders::error));
}

void HyriseServer::start_session(boost::system::error_code error) {
  if (!error) {
    auto connection = std::make_shared<ClientConnection>(std::move(_socket));
    std::make_shared<HyriseSession>(_io_service, connection)->start();
  }

  accept_next_connection();
}

}  // namespace opossum
