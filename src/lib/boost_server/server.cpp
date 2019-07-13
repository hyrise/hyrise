#include "server.hpp"

#include <boost/thread.hpp>
#include <iostream>
#include "tpch/tpch_table_generator.hpp"
namespace opossum {

Server::Server(const uint16_t port)
    : _socket(_io_service), _acceptor(_io_service, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port)) {

    }

void Server::_accept_new_session() {
  auto start_session = boost::bind(&Server::_start_session, this, boost::asio::placeholders::error);
  _acceptor.async_accept(_socket, start_session);
}

void Server::_start_session(boost::system::error_code error) {
  if (!error) {
    boost::thread session_thread([=] {
      // Sockets cannot be copied. After moving the _socket object the object will be in the same state as before.
      auto session = Session(std::move(_socket));
      session.start();
    });
    session_thread.detach();
  }
  _accept_new_session();
}

void Server::run() {
  TpchTableGenerator{0.01f, 100'000}.generate_and_store();
  _accept_new_session();
  std::cout << "Server running on port " << get_port() << std::endl;
  _io_service.run();
}

void Server::shutdown() { _io_service.stop(); }

uint16_t Server::get_port() const { return _acceptor.local_endpoint().port(); }
}  // namespace opossum
