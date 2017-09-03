#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <grpc++/grpc++.h>
#include <grpc/support/log.h>

#include "network/generated/opossum.grpc.pb.h"
#pragma GCC diagnostic pop

namespace opossum {

// Class encompasing the state and logic needed to serve a request.
class RequestHandler {
 public:
  RequestHandler();

  bool has_new_request() const;

  void set_new_request_flag(const bool flag);

  proto::Request& get_request();

  grpc::ServerAsyncResponseWriter<proto::Response>& get_responder();

  grpc::ServerContext& get_server_context();

  // Translates protocol buffer objects into OperatorTasks and schedules them together with a materialization task
  void parse_and_schedule(const bool skip_scheduler);

  // Instructs the gRPC engine to send a response back to the client
  void send_response();

 protected:
  // Context for the RPC, allowing to tweak aspects of it such as the use
  // of compression, authentication, as well as to send metadata back to the
  // client.
  grpc::ServerContext _ctx;

  // What we get from the client.
  proto::Request _request;
  // What we send back to the client.
  proto::Response _response;

  // The means to get back to the client.
  grpc::ServerAsyncResponseWriter<proto::Response> _responder;

  // Is set to 'true' in constructor and set to 'false' the first time it
  bool _has_new_request;
};

}  // namespace opossum
