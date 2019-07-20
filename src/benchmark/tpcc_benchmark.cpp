#include "tpcc/tpcc_table_generator.hpp"

#include "benchmark_runner.hpp"
#include "cli_config_parser.hpp"
#include "tpcc/tpcc_benchmark_item_runner.hpp"
#include "tpcc/tpcc_table_generator.hpp"

using namespace opossum;  // NOLINT

/**
 * This benchmark measures Hyrise's performance executing the TPC-C benchmark. As with the other TPC-* benchmarks, we
 * took some liberty in interpreting the standard. Most notably, all parts about the simulated terminals are ignored.
 * Instead, only the queries leading to the terminal output are executed. In the research world, doing so has become
 * the de facto standard. 
 *
 * main() is mostly concerned with parsing the CLI options while BenchmarkRunner.run() performs the actual benchmark
 * logic.
 */

int main(int argc, char* argv[]) {
  auto cli_options = BenchmarkRunner::get_basic_cli_options("TPC-C Benchmark");

  // clang-format off
  cli_options.add_options()
    // We use -s instead of -w for consistency with the options of our other TPC-x binaries.
    ("s,scale", "Scale factor (warehouses)", cxxopts::value<int>()->default_value("1")); // NOLINT
  // clang-format on

  std::shared_ptr<BenchmarkConfig> config;
  int num_warehouses;

  if (CLIConfigParser::cli_has_json_config(argc, argv)) {
    // JSON config file was passed in
    const auto json_config = CLIConfigParser::parse_json_config_file(argv[1]);
    num_warehouses = json_config.value("scale", 1);

    config = std::make_shared<BenchmarkConfig>(CLIConfigParser::parse_basic_options_json_config(json_config));
  } else {
    // Parse regular command line args
    const auto cli_parse_result = cli_options.parse(argc, argv);

    if (CLIConfigParser::print_help_if_requested(cli_options, cli_parse_result)) return 0;

    num_warehouses = cli_parse_result["scale"].as<int>();

    config = std::make_shared<BenchmarkConfig>(CLIConfigParser::parse_basic_cli_options(cli_parse_result));
  }

  auto context = BenchmarkRunner::create_context(*config);

  std::cout << "- TPC-C scale factor (number of warehouses) is " << num_warehouses << std::endl;

  // Add TPC-C-specific information
  context.emplace("scale_factor", num_warehouses);

  // Run the benchmark
  auto item_runner = std::make_unique<TPCCBenchmarkItemRunner>(config, num_warehouses);
  BenchmarkRunner(*config, std::move(item_runner), std::make_unique<TPCCTableGenerator>(num_warehouses, config), context)
      .run();
}
