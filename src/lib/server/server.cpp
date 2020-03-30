#include "server.hpp"

#include <pthread.h>

#include <iostream>
#include <thread>

namespace opossum {

// Specified port (default: 5432) will be opened after initializing the _acceptor
Server::Server(const boost::asio::ip::address& address, const uint16_t port,
               const SendExecutionInfo send_execution_info)
    : _acceptor(_io_service, boost::asio::ip::tcp::endpoint(address, port)), _send_execution_info(send_execution_info) {
  std::cout << "Server started at " << server_address() << " and port " << server_port() << std::endl
            << "Run 'psql -h localhost' to connect to the server" << std::endl;
}

void Server::run() {
  _accept_new_session();
  _io_service.run();
}

void Server::_accept_new_session() {
  // Create a new session. This will also open a new data socket in order to communicate with the client
  // For more information on TCP ports + Asio see:
  // https://www.gamedev.net/forums/topic/586557-boostasio-allowing-multiple-connections-to-a-single-server-socket/
  auto new_session = std::make_shared<Session>(_io_service, _send_execution_info);
  _acceptor.async_accept(*(new_session->socket()),
                         boost::bind(&Server::_start_session, this, new_session, boost::asio::placeholders::error));
}

void Server::_start_session(const std::shared_ptr<Session>& new_session, const boost::system::error_code& error) {
  Assert(!error, error.message());

  std::thread session_thread([session = new_session, &num_running_sessions = this->_num_running_sessions]() mutable {
    const std::string thread_name = "server_p_" + std::to_string(session->socket()->remote_endpoint().port());
#ifdef __APPLE__
    pthread_setname_np(thread_name.c_str());
#elif __linux__
    pthread_setname_np(pthread_self(), thread_name.c_str());
#endif

    ++num_running_sessions;

    session->run();

    // Destroy the session before reducing the number of running sessions. This makes sure that the server has not shut
    // down yet. In case session.use_count is not yet zero, this makes sure that the destructor is not called in
    // the detached thread.
    session.reset();
    --num_running_sessions;
  });

  session_thread.detach();
  _accept_new_session();
}

boost::asio::ip::address Server::server_address() const { return _acceptor.local_endpoint().address(); }

uint16_t Server::server_port() const { return _acceptor.local_endpoint().port(); }

void Server::shutdown() {
  while (_num_running_sessions > 0) {
    // This busy wait might be inefficient, but as this is only to guarantee a clean shutdown, it's good enough.
    std::this_thread::yield();
  }
  _io_service.stop();
}

}  // namespace opossum
