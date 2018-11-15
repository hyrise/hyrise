#include <boost/algorithm/string.hpp>
#include <cxxopts.hpp>

#include "benchmark_runner.hpp"
#include "benchmark_utils.hpp"
#include "file_based_query_generator.hpp"
#include "import_export/csv_parser.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"
#include "utils/are_args_cxxopts_compatible.hpp"
#include "utils/filesystem.hpp"
#include "utils/load_table.hpp"
#include "utils/performance_warning.hpp"

namespace {

using namespace opossum;  // NOLINT

void _load_table_folder(const BenchmarkConfig& config, const std::string& table_path) {
  const auto is_table_file = [](const std::string& filename) {
    return (boost::algorithm::ends_with(filename, ".csv") || boost::algorithm::ends_with(filename, ".tbl"));
  };

  filesystem::path path{table_path};
  Assert(filesystem::exists(path), "No such file or directory '" + table_path + "'");

  std::vector<std::string> tables;

  // If only one file was specified, add it and return
  if (filesystem::is_regular_file(path)) {
    Assert(is_table_file(table_path), "Specified file '" + table_path + "' is not a .csv or .tbl file");
    tables.push_back(table_path);
  } else {
    // Recursively walk through the specified directory and add all files on the way
    for (const auto& entry : filesystem::recursive_directory_iterator(path)) {
      const auto filename = entry.path().string();
      if (filesystem::is_regular_file(entry) && is_table_file(filename)) {
        tables.push_back(filename);
      }
    }
  }

  Assert(!tables.empty(), "No tables found in '" + table_path + "'");

  for (const auto& table_path_str : tables) {
    const auto table_name = filesystem::path{table_path_str}.stem().string();

    std::shared_ptr<Table> table;
    if (boost::algorithm::ends_with(table_path_str, ".tbl")) {
      table = load_table(table_path_str, config.chunk_size);
    } else {
      table = CsvParser{}.parse(table_path_str);

      // We want to avoid any confusion when the benchmark and the CSV meta file define different chunk sizes.
      // It would be possible for one of these to take precedence and generate a warning in case of a mismatch, but
      // right now, we don't know what the desired behavior is.
      Assert(table->max_chunk_size() == config.chunk_size,
             std::string("Maximum chunk size defined in benchmark (") + std::to_string(config.chunk_size) +
                 ") and in csv meta file (" + std::to_string(table->max_chunk_size()) + ") differ");
    }

    config.out << "- Adding table '" << table_name << "'" << std::endl;
    BenchmarkTableEncoder::encode(table_name, table, config.encoding_config);
    StorageManager::get().add_table(table_name, table);
  }
}

}  // namespace

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
    Assert(opossum::are_args_cxxopts_compatible(argc, argv), "Command line argument incompatible with cxxopts");
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

  // Load the data
  _load_table_folder(*config, table_path);

  // Run the benchmark
  auto context = opossum::BenchmarkRunner::create_context(*config);
  opossum::BenchmarkRunner{*config, std::make_unique<FileBasedQueryGenerator>(*config, query_path), context}.run();
}
