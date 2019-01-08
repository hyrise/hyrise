#include <boost/algorithm/string.hpp>
#include <cxxopts.hpp>

#include "benchmark_runner.hpp"
#include "cli_config_parser.hpp"
#include "file_based_query_generator.hpp"
#include "file_based_table_generator.hpp"
#include "import_export/csv_parser.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/filesystem.hpp"
#include "utils/load_table.hpp"
#include "utils/performance_warning.hpp"

using namespace opossum;  // NOLINT

int main(int argc, char* argv[]) {
  auto cli_options = BenchmarkRunner::get_basic_cli_options("Hyrise Benchmark Runner");

  // clang-format off
  cli_options.add_options()
      ("tables", "Specify directory from which tables are loaded", cxxopts::value<std::string>()->default_value("")) // NOLINT
      ("queries", "Specify queries to run, either a single .sql file or a directory with these files", cxxopts::value<std::string>()->default_value("")); // NOLINT
  // clang-format on

  std::shared_ptr<BenchmarkConfig> benchmark_config;
  std::string query_path;
  std::string table_path;

  if (CLIConfigParser::cli_has_json_config(argc, argv)) {
    // JSON config file was passed in
    const auto json_config = CLIConfigParser::parse_json_config_file(argv[1]);
    table_path = json_config.value("tables", "");
    query_path = json_config.value("queries", "");

    benchmark_config = std::make_shared<BenchmarkConfig>(CLIConfigParser::parse_basic_options_json_config(json_config));

  } else {
    // Parse regular command line args
    const auto cli_parse_result = cli_options.parse(argc, argv);

    // Display usage and quit
    if (cli_parse_result.count("help")) {
      std::cout << CLIConfigParser::detailed_help(cli_options) << std::endl;
      return 0;
    }

    query_path = cli_parse_result["queries"].as<std::string>();
    table_path = cli_parse_result["tables"].as<std::string>();

    benchmark_config = std::make_shared<BenchmarkConfig>(CLIConfigParser::parse_basic_cli_options(cli_parse_result));
  }

  // Check that the options 'queries' and 'tables' were specifiedc
  if (query_path.empty() || table_path.empty()) {
    std::cerr << "Need to specify --queries=path/to/queries and --tables=path/to/tables" << std::endl;
    std::cerr << cli_options.help({}) << std::endl;
    return 1;
  }

  benchmark_config->out << "- Benchmarking queries from " << query_path << std::endl;
  benchmark_config->out << "- Running on tables from " << table_path << std::endl;

  // Run the benchmark
  auto context = BenchmarkRunner::create_context(*benchmark_config);
  auto table_generator = std::make_unique<FileBasedTableGenerator>(benchmark_config, table_path);
  auto query_generator = std::make_unique<FileBasedQueryGenerator>(*benchmark_config, query_path);

  BenchmarkRunner{*benchmark_config, std::move(query_generator), std::move(table_generator), context}.run();
}
