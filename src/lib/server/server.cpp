#include "server.hpp"

#include <pthread.h>

#include <iostream>
#include <thread>

#include "hyrise.hpp"
#include "scheduler/node_queue_scheduler.hpp"

namespace opossum {

// Specified port (default: 5432) will be opened after initializing the _acceptor
Server::Server(const boost::asio::ip::address& address, const uint16_t port,
               const SendExecutionInfo send_execution_info)
    : _acceptor(_io_service, boost::asio::ip::tcp::endpoint(address, port)), _send_execution_info(send_execution_info) {
  std::cout << "Server started at " << server_address() << " and port " << server_port() << std::endl
            << "Run 'psql -h localhost " << server_address() << "' to connect to the server" << std::endl;
}

void Server::run() {
  _is_initialized = false;

  // Set scheduler so that the server can execute the tasks on separate threads.
  Hyrise::get().set_scheduler(std::make_shared<opossum::NodeQueueScheduler>());

  // Set caches
  Hyrise::get().default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
  Hyrise::get().default_lqp_cache = std::make_shared<SQLLogicalPlanCache>();

  _is_initialized = true;
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

  // We ensure that all threads are completed before the server is shut down by tracking the number of running threads
  // in _num_running_sessions. The alternative would be to store the std::thread objects in some kind of data structure
  // and call join() on all of them before shutting down the server. This would result in a growing number of completed
  // threads waiting to be joined and the need for a cleanup procedure.
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

bool Server::is_initialized() const { return _is_initialized; }

}  // namespace opossum
