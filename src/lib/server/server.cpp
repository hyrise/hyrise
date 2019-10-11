#include "server.hpp"

#include <iostream>
#include <thread>

namespace opossum {

// Specified port (default: 5432) will be opened after initializing the _acceptor
Server::Server(const boost::asio::ip::address& address, const uint16_t port, const bool send_execution_info)
    : _acceptor(_io_service, boost::asio::ip::tcp::endpoint(address, port)),
      _send_execution_info(send_execution_info) {
  std::cout << "Server starting using address " << get_address() << " and port " << get_port() << std::endl;
}

void Server::_accept_new_session() {
  // Create a new session. This will also open a new data socket in order to communicate with the client
  // For more information on TCP ports + Asio see:
  // https://www.gamedev.net/forums/topic/586557-boostasio-allowing-multiple-connections-to-a-single-server-socket/
  auto new_session = std::make_shared<Session>(_io_service, _send_execution_info);
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

boost::asio::ip::address Server::get_address() const { return _acceptor.local_endpoint().address(); }

}  // namespace opossum
