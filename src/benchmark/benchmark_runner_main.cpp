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
  auto cli_options = opossum::BenchmarkRunner::get_default_cli_options("Hyrise Benchmark Runner");

  // clang-format off
  cli_options.add_options()
      ("tables", "Specify tables to load, either a single .csv/.tbl file or a directory with these files", cxxopts::value<std::string>()) // NOLINT
      ("queries", "Specify queries to run, either a single .sql file or a directory with these files", cxxopts::value<std::string>()); // NOLINT
  // clang-format on

  const auto cli_parse_result = cli_options.parse(argc, argv);

  // Display usage and quit
  if (cli_parse_result.count("help")) {
    std::cout << cli_options.help({}) << std::endl;
    return 0;
  }

  const bool verbose = cli_parse_result["verbose"].as<bool>();
  auto& out = opossum::get_out_stream(verbose);

  // Check that the options 'queries' and 'tables' were specifiedc
  if (cli_parse_result.count("queries") == 0 || cli_parse_result.count("tables") == 0) {
    std::cerr << "Need to specify --queries=path/to/queries and --tables=path/to/tables" << std::endl;
    std::cerr << cli_options.help({}) << std::endl;
    return 1;
  }

  const auto config = opossum::BenchmarkRunner::parse_default_cli_options(cli_parse_result, cli_options);

  const auto query_path = cli_parse_result["queries"].as<std::string>();
  out << "- Benchmarking queries from " << query_path << std::endl;

  const auto table_path = cli_parse_result["tables"].as<std::string>();
  out << "- Running on tables from " << table_path << std::endl;

  // Run the benchmark
  opossum::BenchmarkRunner::create(config, table_path, query_path).run();
}
