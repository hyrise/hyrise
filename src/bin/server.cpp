#include "server/server.hpp"

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/constants.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/system/detail/error_code.hpp>

#include "cxxopts.hpp"

#include "benchmark_config.hpp"
#include "server/server_types.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "tpcds/tpcds_table_generator.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "utils/assert.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

void generate_benchmark_data(std::string argument_string) {
  // Remove unnecessary whitespaces.
  boost::trim_if(argument_string, boost::is_any_of(":"));

  // Remove dashes and convert to lower case to unify different writings of benchmarks ("TPC-H", "tpch", or "tpc-h").
  boost::replace_all(argument_string, "-", "");
  boost::to_lower(argument_string);

  auto benchmark_data_config = std::vector<std::string>{};
  // Split benchmark name and scale factor.
  boost::split(benchmark_data_config, argument_string, boost::is_any_of(":"), boost::token_compress_on);
  Assert(benchmark_data_config.size() == 2,
         "Malformed input for benchmark data generation. Expecting <benchmark name>:<scale factor>.");

  const auto benchmark_name = benchmark_data_config[0];
  const auto scale_factor = boost::lexical_cast<float, std::string>(benchmark_data_config[1]);

  Assert(benchmark_name == "tpch" || benchmark_name == "tpcds" || benchmark_name == "tpcc",
         "Benchmark data generation is only supported for TPC-C, TPC-DS, and TPC-H.");

  constexpr auto CACHE_BINARY_TABLES = true;
  const auto config = std::make_shared<BenchmarkConfig>(Chunk::DEFAULT_SIZE, CACHE_BINARY_TABLES);
  if (benchmark_name == "tpcc") {
    TPCCTableGenerator{static_cast<uint32_t>(scale_factor), config}.generate_and_store();
  } else if (benchmark_name == "tpcds") {
    TPCDSTableGenerator{static_cast<uint32_t>(scale_factor), config}.generate_and_store();
  } else if (benchmark_name == "tpch") {
    TPCHTableGenerator{scale_factor, ClusteringConfiguration::None, config}.generate_and_store();
  } else {
    Fail("Unexpected benchmark name passed in parameter 'benchmark_data'.");
  }
}

}  // namespace

cxxopts::Options get_server_cli_options() {
  auto cli_options = cxxopts::Options{"./hyriseServer", "Starts Hyrise server in order to accept network requests."};

  // clang-format off
  cli_options.add_options()
    ("help", "Display this help and exit")
    ("address", "Specify the address to run on", cxxopts::value<std::string>()->default_value("0.0.0.0"))
    ("p,port", "Specify the port number. 0 means randomly select an available one. If no port is specified, the "
               "server will start on PostgreSQL's official port", cxxopts::value<uint16_t>()->default_value("5432"))
    ("benchmark_data", "Optional for benchmarking purposes: specify the benchmark name and sizing factor to generate "
                       "at server start (e.g., \"TPC-C:5\", \"TPC-DS:5\", or \"TPC-H:10\"). Supported are TPC-C, "
                       "TPC-DS, and TPC-H. The sizing factor determines the scale factor in TPC-DS and TPC-H, and the "
                       "warehouse count in TPC-C.", cxxopts::value<std::string>())
    ("execution_info", "Send execution information after statement execution", cxxopts::value<bool>()->default_value("false"));  // NOLINT(whitespace/line_length)
  // clang-format on

  return cli_options;
}

int main(int argc, char* argv[]) {
  auto cli_options = get_server_cli_options();
  const auto parsed_options = cli_options.parse(argc, argv);

  // Print help and exit.
  if (parsed_options.count("help") > 0) {
    std::cout << cli_options.help() << '\n';
    return 0;
  }

  /**
    * The optional parameter `benchmark_data` allows users to generate benchmark data when starting the Hyrise server.
    * This is not an ideal solution, but due to several users' requests and our goal to facilitate easy evaluation of
    * Hyrise, we decided to integrate the data generation into the server nonetheless.
    *
    * We do not plan on exposing other parameters, such as the encoding or the chunk size via this facility. You can
    * modify the config object as needed.
    */
  if (parsed_options.count("benchmark_data") > 0) {
    generate_benchmark_data(parsed_options["benchmark_data"].as<std::string>());
  }

  const auto execution_info = parsed_options["execution_info"].as<bool>();
  const auto port = parsed_options["port"].as<uint16_t>();

  auto error = boost::system::error_code{};
  const auto address = boost::asio::ip::make_address(parsed_options["address"].as<std::string>(), error);

  Assert(!error, "Not a valid IPv4 address: " + parsed_options["address"].as<std::string>() + ", terminating...");

  auto server = hyrise::Server{address, port, static_cast<hyrise::SendExecutionInfo>(execution_info)};
  server.run();

  return 0;
}
