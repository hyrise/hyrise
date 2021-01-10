#include "cxxopts.hpp"

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "benchmark_config.hpp"
#include "server/server.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "tpcds/tpcds_table_generator.hpp"

cxxopts::Options get_server_cli_options() {
  cxxopts::Options cli_options("./hyriseServer", "Starts Hyrise server in order to accept network requests.");

  // clang-format off
  cli_options.add_options()
    ("help", "Display this help and exit") // NOLINT
    ("address", "Specify the address to run on", cxxopts::value<std::string>()->default_value("0.0.0.0"))  // NOLINT
    ("p,port", "Specify the port number. 0 means randomly select an available one. If no port is specified, the the server will start on PostgreSQL's official port", cxxopts::value<uint16_t>()->default_value("5432"))  // NOLINT
    ("benchmark_data", "Specify the benchmark and scale factor to generate at server start (e.g., " // NOLINT
                       "\"TPC-H:10\" or \"TPC-DS:5\"; supported are TPC-H and TPC-DS, scale factor is an integer)", cxxopts::value<std::string>()) // NOLINT
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
    * The following code handles the parameter `benchmark_data` which allows users to generate benchmark data when
    * starting the hyrise server. This is not an ideal solution, but due to several users requests and our goal to
    * facilitate easy evaluation of hyrise, we decided to integrate the data generation in to the server nonetheless.
    */
  if (parsed_options.count("benchmark_data")) {
    auto benchmark_data_arg = parsed_options["benchmark_data"].as<std::string>();
    boost::trim_if(benchmark_data_arg, boost::is_any_of(":"));

    std::vector<std::string> bechmark_data_config;
    boost::split(bechmark_data_config, benchmark_data_arg, boost::is_any_of(":"), boost::token_compress_on);
    Assert(bechmark_data_config.size() == 2, "Malformed input for parameter 'benchmark_data'.");

    auto benchmark_str = bechmark_data_config[0];
    std::transform(benchmark_str.begin(), benchmark_str.end(), benchmark_str.begin(), [](unsigned char c){ return std::tolower(c); });
    const auto scale_factor = boost::lexical_cast<uint32_t, std::string>(bechmark_data_config[1]);

    auto config = std::make_shared<opossum::BenchmarkConfig>(opossum::BenchmarkConfig::get_default_config());
    config->cache_binary_tables = true;
    if (benchmark_str == "tpc-h" || benchmark_str == "tpch") {
      opossum::TPCHTableGenerator{static_cast<float>(scale_factor), config}.generate_and_store();
    } else if (benchmark_str == "tpc-ds" || benchmark_str == "tpcds") {
      opossum::TPCDSTableGenerator{scale_factor, config}.generate_and_store();
    } else {
      Fail("Unexpected benchmark passed in parameter 'benchmark_data'.");
    }
  }

  const auto execution_info = parsed_options["execution_info"].as<bool>();
  const auto port = parsed_options["port"].as<uint16_t>();

  boost::system::error_code error;
  const auto address = boost::asio::ip::make_address(parsed_options["address"].as<std::string>(), error);

  Assert(!error, "Not a valid IPv4 address: " + parsed_options["address"].as<std::string>() + ", terminating...");

  auto server = opossum::Server{address, port, static_cast<opossum::SendExecutionInfo>(execution_info)};
  server.run();

  return 0;
}
