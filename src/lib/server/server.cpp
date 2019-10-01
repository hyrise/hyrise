#include "server.hpp"

#include <iostream>
#include <thread>

namespace opossum {

// Specified port (default: 5432) will be opened after initializing the _acceptor
Server::Server(const uint16_t port, const bool debug_note)
    : _acceptor(_io_service, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port)),
      _debug_note(debug_note) {
  std::cout << "Server starting on port " << get_port() << std::endl;
}

void Server::_accept_new_session() {
  // Create a new session. This will also open a new data socket in order to communicate with the client
  // For more information on TCP ports + Asio see:
  // https://www.gamedev.net/forums/topic/586557-boostasio-allowing-multiple-connections-to-a-single-server-socket/
  auto new_session = std::make_shared<Session>(_io_service, _debug_note);
  _acceptor.async_accept(*(new_session->get_socket()),
                         boost::bind(&Server::_start_session, this, new_session, boost::asio::placeholders::error));
}

void Server::_start_session(const std::shared_ptr<Session>& new_session, const boost::system::error_code& error) {
  if (!error) {
    std::thread session_thread([new_session] { new_session->start(); });
    session_thread.detach();
  } else {
    std::cerr << error.category().name() << ": " << error.message() << std::endl;
  }
  _accept_new_session();
}

void Server::run() {
  _accept_new_session();
  _io_service.run();
}

void Server::shutdown() { _io_service.stop(); }

uint16_t Server::get_port() const { return _acceptor.local_endpoint().port(); }

}  // namespace opossum
