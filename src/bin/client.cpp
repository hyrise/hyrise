#include "client.hpp"

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/config.hpp>
#include <boost/program_options/environment_iterator.hpp>
#include <boost/program_options/eof_iterator.hpp>
#include <boost/program_options/errors.hpp>
#include <boost/program_options/option.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/version.hpp>

#include <iomanip>
#include <iostream>
#include <memory>
#include <string>

#include "constant_mappings.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace po = boost::program_options;

namespace opossum {

// Sample client showing how to use protobuf and grpc at client-side
// This client uses a synchronous, blocking grpc-call, for an async client sample, see network-tests
OpossumClient::OpossumClient(std::shared_ptr<Channel> channel) : _stub(proto::OpossumService::NewStub(channel)) {}

// Assembles the client's payload, sends it and presents the response back from the server.
void OpossumClient::query(const std::string& table_name, const ColumnID column_id, const proto::ScanType scan_type,
                          const std::string& filter) {
  // Data we are sending to the server.
  proto::Request request;

  proto::GetTableOperator* get_table = nullptr;
  auto root_op_variant = request.mutable_root_operator();

  if (!filter.empty()) {
    // Init a TableScan (protobuf allocates and manages the needed resources)
    proto::TableScanOperator* table_scan = root_op_variant->mutable_table_scan();
    table_scan->set_column_id(column_id);
    table_scan->set_filter_operator(scan_type);
    proto::Variant* variant = table_scan->mutable_value();
    variant->set_value_int(std::stoi(filter));
    // Add a GetTable operator as input operator for TableScan
    get_table = table_scan->mutable_input_operator()->mutable_get_table();
  }

  if (!get_table) {
    // Init a GetTable operator if there is no TableScan already
    get_table = root_op_variant->mutable_get_table();
  }
  get_table->set_table_name(table_name);

  // Container for the data we expect from the server.
  proto::Response response;

  // Context for the client. It could be used to convey extra information to
  // the server and/or tweak certain RPC behaviors.
  ClientContext context;

  // The actual RPC (synchronous).
  Status status = _stub->Query(&context, request, &response);

  // Act upon the status of the actual RPC.
  if (status.ok()) {
    print_response_table(response);
  } else {
    std::cout << "RPC failed" << std::endl;
  }
}

void OpossumClient::print_variant(const proto::Variant& variant) const {
  std::cout << "|" << std::setw(20);
  switch (variant.variant_case()) {
    case proto::Variant::kValueInt:
      std::cout << variant.value_int();
      break;
    case proto::Variant::kValueFloat:
      std::cout << variant.value_float();
      break;
    case proto::Variant::kValueString:
      std::cout << variant.value_string();
      break;
    case proto::Variant::kValueDouble:
      std::cout << variant.value_double();
      break;
    case proto::Variant::kValueLong:
      std::cout << variant.value_long();
      break;
    default:
      Fail("Unknown AllTypeVariant in operator_translator");
  }
  std::cout << std::setw(0);
}

void OpossumClient::print_response_table(proto::Response& response) const {
  if (response.result_case() == proto::Response::kError) {
    std::cout << "Error in request: " << response.error() << std::endl;
    return;
  }

  const auto table = response.response_table();
  std::cout << "=== Columns"
            << " === " << std::endl;
  for (int i = 0; i < table.column_type_size(); ++i) {
    std::cout << "|" << std::setw(20) << table.column_type(i) << std::setw(0);
  }
  std::cout << "|" << std::endl;
  for (int i = 0; i < table.column_name_size(); ++i) {
    std::cout << "|" << std::setw(20) << table.column_name(i) << std::setw(0);
  }
  std::cout << "|" << std::endl;
  std::cout << "=== Values"
            << " === " << std::endl;
  for (int row_id = 0; row_id < table.row_size(); ++row_id) {
    auto row = table.row(row_id);
    for (int i = 0; i < row.value_size(); ++i) {
      auto variant = row.value(i);
      print_variant(variant);
    }
    std::cout << "|" << std::endl;
  }
}

}  // namespace opossum

int main(int argc, char** argv) {
  po::options_description desc("Allowed options");
  auto options = desc.add_options();
  options("help", "print help message");
  options("table_name", po::value<std::string>(), "opossum table name (required)");
  options("address", po::value<std::string>()->default_value("0.0.0.0:50051"), "IP:PORT");
  options("column_id", po::value<opossum::ColumnID>()->default_value(opossum::ColumnID{0}), "column id for table scan");
  options("filter_op", po::value<std::string>()->default_value(""), "filter operation for table scan, e.g. = > >= ...");
  options("filter_val", po::value<std::string>()->default_value(""), "filter value for table scan");

  po::positional_options_description pd;
  pd.add("table_name", 1);
  pd.add("column_id", 1);
  pd.add("filter_op", 1);
  pd.add("filter_val", 1);

  po::variables_map variables;
  po::store(po::command_line_parser(argc, argv).options(desc).positional(pd).run(), variables);
  po::notify(variables);

  if (variables.count("help") || variables.count("table_name") == 0) {
    std::cout << desc << std::endl;
    return 1;
  }

  auto address = variables["address"].as<std::string>();
  auto table_name = variables["table_name"].as<std::string>();
  auto column_id = variables["column_id"].as<opossum::ColumnID>();
  auto filter_op = variables["filter_op"].as<std::string>();
  auto filter = variables["filter_val"].as<std::string>();

  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint. We indicate that the channel isn't authenticated (use
  // of InsecureChannelCredentials()).
  opossum::OpossumClient client(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));

  std::cout << "Sending query to " << address << std::endl;
  client.query(table_name, column_id, opossum::string_to_proto_scan_type.at(filter_op), filter);

  return 0;
}
