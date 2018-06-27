#include <cxxopts.hpp>

#include "benchmark_runner.hpp"
#include "benchmark_utils.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "tpch/tpch_queries.hpp"
#include "types.hpp"
#include "utils/performance_warning.hpp"

int main(int argc, char* argv[]) {
  auto cli_options = opossum::BenchmarkRunner::get_basic_cli_options("Hyrise Benchmark Runner");

  // clang-format off
  cli_options.add_options()
      ("tables", "Specify tables to load, either a single .csv/.tbl file or a directory with these files", cxxopts::value<std::string>()->default_value("")) // NOLINT
      ("queries", "Specify queries to run, either a single .sql file or a directory with these files", cxxopts::value<std::string>()->default_value("")); // NOLINT
  // clang-format on

  std::unique_ptr<opossum::BenchmarkConfig> config;
  std::string query_path;
  std::string table_path;

  if (opossum::CLIConfigParser::cli_has_json_config(argc, argv)) {
    // JSON config file was passed in
    const auto json_config = opossum::CLIConfigParser::parse_json_config_file(argv[1]);
    table_path = json_config.value("tables", "");
    query_path = json_config.value("queries", "");

    config = std::make_unique<opossum::BenchmarkConfig>(
        opossum::CLIConfigParser::parse_basic_options_json_config(json_config));

  } else {
    // Parse regular command line args
    const auto cli_parse_result = cli_options.parse(argc, argv);

    // Display usage and quit
    if (cli_parse_result.count("help")) {
      std::cout << opossum::CLIConfigParser::detailed_help(cli_options) << std::endl;
      return 0;
    }

    query_path = cli_parse_result["queries"].as<std::string>();
    table_path = cli_parse_result["tables"].as<std::string>();

    config =
        std::make_unique<opossum::BenchmarkConfig>(opossum::CLIConfigParser::parse_basic_cli_options(cli_parse_result));
  }

  // Check that the options 'queries' and 'tables' were specifiedc
  if (query_path.empty() || table_path.empty()) {
    std::cerr << "Need to specify --queries=path/to/queries and --tables=path/to/tables" << std::endl;
    std::cerr << cli_options.help({}) << std::endl;
    return 1;
  }

  config->out << "- Benchmarking queries from " << query_path << std::endl;
  config->out << "- Running on tables from " << table_path << std::endl;

  // Run the benchmark
  opossum::BenchmarkRunner::create(*config, table_path, query_path).run();
}
