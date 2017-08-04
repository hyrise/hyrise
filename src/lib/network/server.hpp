#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <grpc++/grpc++.h>
#pragma GCC diagnostic pop
#include <grpc/support/log.h>

#include <memory>
#include <thread>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "network/generated/opossum.grpc.pb.h"
#pragma GCC diagnostic pop

namespace opossum {

struct ServerConfiguration;

class Server {
 public:
  /**
   * Start the server
   * @param config A config to specify server and scheduler parameters
   * @param waiting_server_thread If true, the thread calling this function (normally the main-thread) will be blocking
   * after server start until shutdown. `false` is used in server tests.
   */
  void start(const ServerConfiguration& config, const bool waiting_server_thread = true);
  void stop();

 protected:
  void thread_func_handle_rpcs(const size_t thread_index);
  void create_and_register_request_handler(const size_t thread_index);

  std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> _cqs;
  proto::OpossumService::AsyncService _service;
  std::unique_ptr<grpc::Server> _server;
  std::vector<std::thread> _listener_threads;
  bool _skip_scheduler;
};

}  // namespace opossum
