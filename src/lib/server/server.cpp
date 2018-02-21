#include "server.hpp"

#include <boost/asio/placeholders.hpp>
#include <boost/bind.hpp>

#include "client_connection.hpp"
#include "server_session.hpp"

namespace opossum {

Server::Server(boost::asio::io_service& io_service, uint16_t port)
    : _io_service(io_service), _acceptor(io_service, tcp::endpoint(tcp::v4(), port)), _socket(io_service) {
  accept_next_connection();
}

void Server::accept_next_connection() {
  _acceptor.async_accept(_socket, boost::bind(&Server::start_session, this, boost::asio::placeholders::error));
}

void Server::start_session(boost::system::error_code error) {
  if (!error) {
    auto connection = std::make_shared<ClientConnection>(std::move(_socket));
    // The session will retain a shared_ptr to itself as long as it's alive
    std::make_shared<ServerSession>(_io_service, connection)->start();
  }

  accept_next_connection();
}

}  // namespace opossum
