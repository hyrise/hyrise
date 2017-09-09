#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <grpc++/grpc++.h>
#pragma GCC diagnostic pop
#include <grpc/support/log.h>

#include <memory>
#include <string>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include "network/generated/opossum.grpc.pb.h"
#pragma GCC diagnostic pop

#include "types.hpp"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

namespace opossum {

// Sample client showing how to use protobuf and grpc at client-side
class OpossumClient {
 public:
  explicit OpossumClient(std::shared_ptr<Channel> channel);

  // Assembles the client's payload, sends it and presents the response back from the server.
  void query(const std::string& table_name, const ColumnID column_id, const proto::ScanType scan_type,
             const std::string& filter);

 protected:
  void print_response_table(proto::Response& response) const;
  void print_variant(const proto::Variant& variant) const;
  // Out of the passed in Channel comes the stub, stored here, our view of the
  // server's exposed services.
  std::unique_ptr<proto::OpossumService::Stub> _stub;
};

}  // namespace opossum
