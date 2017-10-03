#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <grpc++/grpc++.h>
#pragma GCC diagnostic pop
#include <grpc/support/log.h>
#include <chrono>
#include <csignal>
#include <fstream>
#include <memory>
#include <string>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include "network/generated/opossum.grpc.pb.h"
#pragma GCC diagnostic pop

#include "network/server.hpp"
#include "network/server_configuration.hpp"
#include "scheduler/topology.hpp"
#include "storage/storage_manager.hpp"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;

namespace {
std::string load_response(const std::string& file_name) {
  std::ifstream infile(file_name);
  std::string line;
  std::stringstream response_text;

  while (std::getline(infile, line)) {
    response_text << line << std::endl;
  }

  return response_text.str();
}
}  // namespace

namespace opossum {

class ServerTest : public BaseTest {
 protected:
  std::shared_ptr<proto::Response> doAsyncRequest(proto::Request& request) {
    std::unique_ptr<proto::OpossumService::Stub> _stub =
        proto::OpossumService::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));

    // Container for the data we expect from the server.
    auto response = std::make_shared<proto::Response>();

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The producer-consumer queue we use to communicate asynchronously with the
    // gRPC runtime.
    CompletionQueue cq;

    // Storage for the status of the RPC upon completion.
    Status status;

    // _stub->AsyncQuery() performs the RPC call, returning an instance we
    // store in "rpc". Because we are using the asynchronous API, we need to
    // hold on to the "rpc" instance in order to get updates on the ongoing RPC.
    std::unique_ptr<ClientAsyncResponseReader<proto::Response>> rpc(_stub->AsyncQuery(&context, request, &cq));

    // Request that, upon completion of the RPC, "response" be updated with the
    // server's response; "status" with the indication of whether the op_variant
    // was successful. Tag the request with the integer 1.
    rpc->Finish(response.get(), &status, reinterpret_cast<void*>(1));
    void* got_tag;
    bool ok = false;
    // Block until the next result is available in the completion queue "cq".
    // The return value of Next should always be checked. This return value
    // tells us whether there is any kind of event or the _cq is shutting down.
    EXPECT_TRUE(cq.Next(&got_tag, &ok));

    // Verify that the result from "cq" corresponds, by its tag, our previous
    // request.
    EXPECT_TRUE(got_tag == reinterpret_cast<void*>(1));
    // ... and that the request was completed successfully. Note that "ok"
    // corresponds solely to the request for updates introduced by Finish().
    EXPECT_TRUE(ok);

    return response;
  }

  std::shared_ptr<proto::Response> doRequest(proto::Request& request) {
    std::unique_ptr<proto::OpossumService::Stub> _stub =
        proto::OpossumService::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));

    // Container for the data we expect from the server.
    auto response = std::make_shared<proto::Response>();

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = _stub->Query(&context, request, response.get());

    EXPECT_TRUE(status.ok());

    return response;
  }

  void SetUp() override {
    auto test_table = load_table("src/test/tables/float.tbl", 2);
    // At this point it would be too complicated to replace the test tables with TableWrapper. Because the TableWrapper
    // operator would have to be translated by the network module.
    StorageManager::get().add_table(table_name, std::move(test_table));

    config.address = address;
    config.num_listener_threads = 1;
    config.skip_scheduler = false;
    config.topology = opossum::Topology::create_fake_numa_topology(8, 4);
  }

  void TearDown() override { server.stop(); }

  std::string address{"0.0.0.0:50052"};
  std::string table_name{"table1"};
  Server server;
  ServerConfiguration config;
};

TEST_F(ServerTest, SendQuerySkipScheduler) {
  config.skip_scheduler = true;
  server.start(config, false);

  proto::Request request;

  auto response = doRequest(request);

  EXPECT_EQ(response->DebugString(), "");
}

TEST_F(ServerTest, SendNoop) {
  server.start(config, false);

  proto::Request request;
  auto response = doRequest(request);

  EXPECT_EQ(response->DebugString(), "");
}

TEST_F(ServerTest, AsyncQuery) {
  server.start(config, false);

  proto::Request request;
  auto root_op_variant = request.mutable_root_operator();
  auto projection = root_op_variant->mutable_projection();
  projection->add_column_id(0);
  auto get_table = projection->mutable_input_operator()->mutable_get_table();
  get_table->set_table_name(table_name);

  auto response = doAsyncRequest(request);

  auto expected_result = load_response("src/test/responses/float.tbl.rsp");

  EXPECT_EQ(response->DebugString(), expected_result);
}

TEST_F(ServerTest, SendInvalidQuery) {
  server.start(config, false);

  proto::Request request;
  request.mutable_root_operator();

  auto response = doRequest(request);

  EXPECT_FALSE(response->has_response_table());
  EXPECT_EQ(response->error(),
            "Operator not set. Missing dependency. Cannot translate proto object to opossum operator.");
}

}  // namespace opossum
