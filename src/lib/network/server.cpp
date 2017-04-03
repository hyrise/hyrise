#include "server.hpp"

#include <algorithm>
#include <atomic>
#include <csignal>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "operators/abstract_operator.hpp"
#include "operators/get_table.hpp"
#include "operators/import_csv.hpp"
#include "operators/print.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

void Server::start(const ServerConfiguration& config, const bool waiting_server_thread) {
  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(config.topology));

  _skip_scheduler = config.skip_scheduler;

  grpc::ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(config.address, grpc::InsecureServerCredentials());
  // Register "_service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *asynchronous* service.
  builder.RegisterService(&_service);
  // Get hold of the completion queues used for the asynchronous communication
  // with the gRPC runtime. We use one queue per thread.
  for (size_t thread_id = 0; thread_id < config.num_listener_threads; ++thread_id) {
    _cqs.push_back(builder.AddCompletionQueue());
  }
  // Finally assemble the grpc server.
  _server = builder.BuildAndStart();
  std::cout << "Server listening on " << config.address << " with " << config.num_listener_threads
            << " listening threads" << std::endl;

  // Start worker threads
  for (size_t thread_id = 0; thread_id < config.num_listener_threads; ++thread_id) {
    _listener_threads.emplace_back(&Server::thread_func_handle_rpcs, this, thread_id);
  }

  if (waiting_server_thread) {
    _server->Wait();
  }
}

void Server::stop() {
  std::cout << std::endl << "Stopping server..." << std::endl;

  _server->Shutdown();

  // Always shutdown the completion queue after the server.
  for (auto& cq : _cqs) {
    // Shutting down completion queue causes clean of created RequestHandler objects
    cq->Shutdown();
  }

  CurrentScheduler::get()->finish();
  CurrentScheduler::set(nullptr);

  for (auto& thread : _listener_threads) {
    thread.join();
  }

  std::cout << "Server stopped" << std::endl;
}

// Every server-listener-thread starts in this function.
//
// A gRPC client request is processed as follows:
//    1. A new RequestHandler instance is created and registered to serve a new incoming client request
//    2. Next() is called on the completion queue, blocking until a client request is incoming or the queue is shutting
//       down
//    3. When a valid request is incoming, the 'tag' is set to a RequestHandler instance pointer and request_ok
//       is set to 'true'
//    4. A new RequestHandler instance is created to server a future client request.
//    5. The current request is parsed (creating OperatorTasks from Protocol Buffer objects) and every Task is scheduled
//    6. A final JobTask is created and scheduled which builds a response (materialize the response table) and notifies
//       gRPC to send the response back to the client finally
//    7. When the client confirms that he received the response, the "request" is put again into the completion queue.
//       When Next() delivers this object, calling has_new_request() on the RequestHandler instance (referenced via the
//       'tag' pointer) results in 'false', so that the RequestHandler instance is deleted.
//       To sum it up, every "request" is passing the completion queue two times. First as a new incoming client
//       request, second as a finished client request to become cleaned up.
//
// Using the RequestHandler instance pointer to tag, i.e. uniquely identify a client request is an elegant way to avoid
// additional data structures. One could use something else (e.g. a UUID) instead of the RequestHandler instance pointer
// to tag a request. But then we need a mapping from UUID to the smart-pointered RequestHandler instance. This would be
// no improvement in avoiding leaks, because we are still depending on gRPC's leak free completion queue processing.
void Server::thread_func_handle_rpcs(const size_t thread_index) {
  // Initially spawn one new RequestHandler instance to serve the first client request.
  create_and_register_request_handler(thread_index);

  void* tag;  // uniquely identifies a request.
  bool request_ok;

  // Block waiting to read the next event from the completion queue. The event is uniquely identified by its tag, which
  // in this case is the memory address of a RequestHandler instance.
  // The return value of Next() should always be checked, because it tells us whether there will be further events in
  // the queue to become processed.
  // When the queue is shut down and empty, Next() returns 'false'. Otherwise, calling Next() still returns 'true'
  // until the queue is empty, but 'request_ok' is set to 'false'. This is needed to clean up all remaining
  // RequestHandler instances in the queue properly.
  while (_cqs[thread_index]->Next(&tag, &request_ok)) {
    RequestHandler* request_handler = static_cast<RequestHandler*>(tag);
    if (request_handler->has_new_request() && request_ok) {
      request_handler->set_new_request_flag(false);
      // Create new RequestHandler to serve a future incoming client request
      create_and_register_request_handler(thread_index);
      // Start processing the current request
      request_handler->parse_and_schedule(_skip_scheduler);
    } else {
      // Response was already sent to client and the request is completed or CompletionQueue was shut down
      // Clean up allocated resources of the request in server
      delete request_handler;
    }
  }
}

void Server::create_and_register_request_handler(const size_t thread_index) {
  // Spawn a new "empty" RequestHandler instance.
  // The instance will be deleted after response was sent back to the client or CompletionQueue was shut down
  RequestHandler* request_handler = new RequestHandler();

  // We register the "empty" RequestHandler instance to serve a new client request. The request_handler pointer acts as
  // the tag uniquely identifying the request (so that different RequestHandler instances can serve different requests
  // concurrently), in this case the memory address of this RequestHandler instance.
  grpc::ServerCompletionQueue* completion_queue = _cqs[thread_index].get();
  _service.RequestQuery(&request_handler->get_server_context(), &request_handler->get_request(),
                        &request_handler->get_responder(), completion_queue, completion_queue, request_handler);
}

}  // namespace opossum
