#include "cxxopts.hpp"

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "benchmark_config.hpp"
#include "cli_config_parser.hpp"
#include "server/server.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "tpcds/tpcds_table_generator.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch/tpch_table_generator.hpp"

namespace {

void generate_benchmark_data(std::string argument_string) {
  // Remove unnecessary white spaces
  boost::trim_if(argument_string, boost::is_any_of(":"));

  // Remove dashes and convert to lower case to unify different writings of benchmarks ("TPC-H", "tpch", or "tpc-h")
  boost::replace_all(argument_string, "-", "");
  boost::to_lower(argument_string);

  auto benchmark_data_config = std::vector<std::string>{};
  // Split benchmark name and sizing factor
  boost::split(benchmark_data_config, argument_string, boost::is_any_of(":"), boost::token_compress_on);
  Assert(benchmark_data_config.size() == 2,
         "Malformed input for benchmark data generation. Expecting a benchmark name and a sizing factor.");

  const auto benchmark_name = benchmark_data_config[0];
  const auto sizing_factor = boost::lexical_cast<float, std::string>(benchmark_data_config[1]);

  Assert(benchmark_name == "tpch" || benchmark_name == "tpcds" || benchmark_name == "tpcc",
         "Benchmark data generation is only supported for TPC-C, TPC-DS, and TPC-H.");

  auto config = std::make_shared<hyrise::BenchmarkConfig>(hyrise::BenchmarkConfig::get_default_config());
  config->cache_binary_tables = true;
  if (benchmark_name == "tpcc") {
    hyrise::TPCCTableGenerator{static_cast<uint32_t>(sizing_factor), config}.generate_and_store();
  } else if (benchmark_name == "tpcds") {
    hyrise::TPCDSTableGenerator{static_cast<uint32_t>(sizing_factor), config}.generate_and_store();
  } else if (benchmark_name == "tpch") {
    hyrise::TPCHTableGenerator{sizing_factor, ClusteringConfiguration::None, config}.generate_and_store();
  } else {
    Fail("Unexpected benchmark name passed in parameter 'benchmark_data'.");
  }
}

}  // namespace

cxxopts::Options get_server_cli_options() {
  cxxopts::Options cli_options("./hyriseServer", "Starts Hyrise server in order to accept network requests.");

  // clang-format off
  cli_options.add_options()
    ("help", "Display this help and exit") // NOLINT
    ("address", "Specify the address to run on", cxxopts::value<std::string>()->default_value("0.0.0.0"))  // NOLINT
    ("p,port", "Specify the port number. 0 means randomly select an available one. If no port is specified, the the server will start on PostgreSQL's official port", cxxopts::value<uint16_t>()->default_value("5432"))  // NOLINT
    ("benchmark_data", "Optional for benchmarking purposes: specify the benchmark name and sizing factor to generate "
                       "at server start (e.g., \"TPC-C:5\", \"TPC-DS:5\", or \"TPC-H:10\"). Supported are TPC-C, "
                       "TPC-DS, and TPC-H. The sizing factor determines the scale factor in TPC-DS and TPC-H, and the "
                       "warehouse count in TPC-C.", cxxopts::value<std::string>()) // NOLINT
    ("execution_info", "Send execution information after statement execution", cxxopts::value<bool>()->default_value("false")) // NOLINT
    ;  // NOLINT
  // clang-format on

  return cli_options;
}

int main(int argc, char* argv[]) {
  auto cli_options = get_server_cli_options();
  const auto parsed_options = cli_options.parse(argc, argv);

  // Print help and exit
  if (parsed_options.count("help")) {
    std::cout << cli_options.help() << std::endl;
    return 0;
  }

  /**
    * The optional parameter `benchmark_data` allows users to generate benchmark data when starting the hyrise server.
    * This is not an ideal solution, but due to several users' requests and our goal to facilitate easy evaluation of
    * Hyrise, we decided to integrate the data generation into the server nonetheless.
    *
    * We do not plan on exposing other parameters, such as the encoding or the chunk size via this facility. You can
    * change the modify the config object as needed.
    */
  if (parsed_options.count("benchmark_data")) {
    generate_benchmark_data(parsed_options["benchmark_data"].as<std::string>());
  }

  const auto execution_info = parsed_options["execution_info"].as<bool>();
  const auto port = parsed_options["port"].as<uint16_t>();

  boost::system::error_code error;
  const auto address = boost::asio::ip::make_address(parsed_options["address"].as<std::string>(), error);

  Assert(!error, "Not a valid IPv4 address: " + parsed_options["address"].as<std::string>() + ", terminating...");

  auto server = hyrise::Server{address, port, static_cast<hyrise::SendExecutionInfo>(execution_info)};
  server.run();

  return 0;
}
