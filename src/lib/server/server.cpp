#include "server.hpp"

#include <boost/asio/placeholders.hpp>
#include <boost/bind.hpp>

#include "client_connection.hpp"
#include "server_session.hpp"
#include "task_runner.hpp"
#include "then_operator.hpp"

namespace opossum {

using opossum::then_operator::then;

Server::Server(boost::asio::io_service& io_service, uint16_t port)
    : _io_service(io_service),
      _acceptor(io_service, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port)),
      _socket(io_service) {
  _accept_next_connection();
}

void Server::_accept_next_connection() {
  _acceptor.async_accept(_socket, boost::bind(&Server::_start_session, this, boost::asio::placeholders::error));
}

void Server::_start_session(boost::system::error_code error) {
  if (!error) {
    auto connection = std::make_shared<ClientConnection>(std::move(_socket));
    auto task_runner = std::make_shared<TaskRunner>(_io_service);
    auto session = std::make_shared<ServerSession>(connection, task_runner);
    // Start the session and release it once it has terminated
    session->start() >> then >> [=]() mutable { session.reset(); };
  }

  _accept_next_connection();
}

uint16_t Server::get_port_number() { return _acceptor.local_endpoint().port(); }

}  // namespace opossum
