#include "request_handler.hpp"

#include <memory>
#include <utility>

#include "operator_translator.hpp"
#include "response_builder.hpp"

#include "scheduler/job_task.hpp"
#include "scheduler/operator_task.hpp"

namespace opossum {

// Take in the "service" instance (in this case representing an asynchronous
// server) and the completion queue "cq" used for asynchronous communication
// with the gRPC runtime.
RequestHandler::RequestHandler() : _responder(&_ctx), _has_new_request(true) {}

bool RequestHandler::has_new_request() const { return _has_new_request; }

void RequestHandler::set_new_request_flag(const bool flag) { _has_new_request = flag; }

proto::Request& RequestHandler::get_request() { return _request; }

grpc::ServerContext& RequestHandler::get_server_context() { return _ctx; }

grpc::ServerAsyncResponseWriter<proto::Response>& RequestHandler::get_responder() { return _responder; }

void RequestHandler::parse_and_schedule(const bool skip_scheduler) {
  // The actual processing.
  if (skip_scheduler) {
    // Skip scheduling and send response immediately
    send_response();
    return;
  }

  // Empty request - a NOOP is scheduled
  if (!_request.has_root_operator()) {
    auto noop_job = std::make_shared<opossum::JobTask>([this]() { send_response(); });
    noop_job->schedule();
    return;
  }

  // Parsing of invalid request raises an exception
  try {
    OperatorTranslator translator;
    const auto& tasks = translator.build_tasks_from_proto(_request.root_operator());
    auto root_task = translator.root_task();
    auto root_operator = root_task->get_operator();
    auto materialize_job = std::make_shared<opossum::JobTask>([this, root_operator]() {
      // These lines are executed by the opossum scheduler
      auto table = root_operator->get_output();
      // Materialize and fill response
      ResponseBuilder response_builder;
      response_builder.build_response(_response, std::move(table));

      send_response();
    });
    root_task->set_as_predecessor_of(materialize_job);

    // Schedule all tasks
    for (const auto& task : tasks) {
      task->schedule();
    }
    materialize_job->schedule();
  } catch (const std::exception& e) {
    if (IS_DEBUG) {
      std::cerr << "Exception: " << e.what() << std::endl;
    }
    _response.set_error(e.what());
    send_response();
  }
}

void RequestHandler::send_response() {
  // And we are done! Let the gRPC runtime know we've finished, using the
  // memory address of this instance as the uniquely identifying tag for
  // the event.
  _responder.Finish(_response, grpc::Status::OK, this);
}

}  // namespace opossum
