#include <boost/algorithm/string.hpp>
#include <cxxopts.hpp>
#include <filesystem>

#include <fstream>

#include "benchmark_runner.hpp"
#include "cli_config_parser.hpp"
#include "file_based_benchmark_item_runner.hpp"
#include "file_based_table_generator.hpp"
#include "hyrise.hpp"
#include "import_export/csv_parser.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"
#include "utils/performance_warning.hpp"
#include "utils/sqlite_add_indices.hpp"
#include "utils/sqlite_wrapper.hpp"
#include "utils/timer.hpp"

/**
 * The Join Order Benchmark was introduced by Leis et al. "How good are query optimizers, really?".
 * It runs on an IMDB database from ~2013 that gets downloaded if necessary as part of running this benchmark.
 * Its 113 queries are obtained from the "third_party/join-order-benchmark" submodule
 */

using namespace opossum;               // NOLINT
using namespace std::string_literals;  // NOLINT

int main(int argc, char* argv[]) {
  auto cli_options = BenchmarkRunner::get_basic_cli_options("Hyrise Join Order Benchmark");

  const auto DEFAULT_TABLE_PATH = "imdb_data";
  const auto DEFAULT_QUERY_PATH = "third_party/join-order-benchmark";

  // clang-format off
  cli_options.add_options()
  ("table_path", "Directory containing the Tables as csv, tbl or binary files. CSV files require meta-files, see csv_meta.hpp or any *.csv.json file.", cxxopts::value<std::string>()->default_value(DEFAULT_TABLE_PATH)) // NOLINT
  ("query_path", "Directory containing the .sql files of the Join Order Benchmark", cxxopts::value<std::string>()->default_value(DEFAULT_QUERY_PATH)) // NOLINT
  ("q,queries", "Subset of queries to run as a comma separated list", cxxopts::value<std::string>()->default_value("all")); // NOLINT
  // clang-format on

  std::shared_ptr<BenchmarkConfig> benchmark_config;
  std::string query_path;
  std::string table_path;
  // Comma-separated query names or "all"
  std::string queries_str;

  if (CLIConfigParser::cli_has_json_config(argc, argv)) {
    // JSON config file was passed in
    const auto json_config = CLIConfigParser::parse_json_config_file(argv[1]);
    table_path = json_config.value("table_path", DEFAULT_TABLE_PATH);
    query_path = json_config.value("query_path", DEFAULT_QUERY_PATH);
    queries_str = json_config.value("queries", "all");

    benchmark_config = std::make_shared<BenchmarkConfig>(CLIConfigParser::parse_basic_options_json_config(json_config));

  } else {
    // Parse regular command line args
    const auto cli_parse_result = cli_options.parse(argc, argv);

    if (CLIConfigParser::print_help_if_requested(cli_options, cli_parse_result)) return 0;

    query_path = cli_parse_result["query_path"].as<std::string>();
    table_path = cli_parse_result["table_path"].as<std::string>();
    queries_str = cli_parse_result["queries"].as<std::string>();

    benchmark_config = std::make_shared<BenchmarkConfig>(CLIConfigParser::parse_basic_cli_options(cli_parse_result));
  }

  // Check that the options "query_path" and "table_path" were specified
  if (query_path.empty() || table_path.empty()) {
    std::cerr << "Need to specify --query_path=path/to/queries and --table_path=path/to/table_files" << std::endl;
    std::cerr << cli_options.help({}) << std::endl;
    return 1;
  }

  /**
   * Use a Python script to download and unzip the IMDB. We do this in Python and not in C++ because downloading and
   * unzipping is straight forward in Python (and we suspect in C++ it might be... cumbersome).
   */
  const auto setup_imdb_command = "python3 scripts/setup_imdb.py "s + table_path;
  const auto setup_imdb_return_code = system(setup_imdb_command.c_str());
  Assert(setup_imdb_return_code == 0, "setup_imdb.py failed. Did you run the benchmark from the project root dir?");

  // The join-order-benchmark ships with these two .sql scripts, but we do not want to run them as part of the benchmark
  // as they do not contains actual queries
  const auto non_query_file_names = std::unordered_set<std::string>{"fkindexes.sql", "schema.sql"};

  std::cout << "- Benchmarking queries from " << query_path << std::endl;
  std::cout << "- Running on tables from " << table_path << std::endl;

  std::optional<std::unordered_set<std::string>> query_subset;
  if (queries_str == "all") {
    std::cout << "- Running all queries from specified path" << std::endl;
  } else {
    std::cout << "- Running subset of queries: " << queries_str << std::endl;

    // "a, b, c, d" -> ["a", " b", " c", " d"]
    auto query_subset_untrimmed = std::vector<std::string>{};
    boost::algorithm::split(query_subset_untrimmed, queries_str, boost::is_any_of(","));

    // ["a", " b", " c", " d"] -> ["a", "b", "c", "d"]
    query_subset.emplace();
    for (auto& query_name : query_subset_untrimmed) {
      query_subset->emplace(boost::trim_copy(query_name));
    }
  }

  // Run the benchmark
  auto context = BenchmarkRunner::create_context(*benchmark_config);
  auto table_generator = std::make_unique<FileBasedTableGenerator>(benchmark_config, table_path);
  auto benchmark_item_runner =
      std::make_unique<FileBasedBenchmarkItemRunner>(benchmark_config, query_path, non_query_file_names, query_subset);

  if (benchmark_item_runner->items().empty()) {
    std::cout << "No items to run.\n";
    return 1;
  }

  auto benchmark_runner = std::make_shared<BenchmarkRunner>(*benchmark_config, std::move(benchmark_item_runner),
                                                            std::move(table_generator), context);
  Hyrise::get().benchmark_runner = benchmark_runner;

  if (benchmark_config->verify) {
    add_indices_to_sqlite(query_path + "/schema.sql", query_path + "/fkindexes.sql", benchmark_runner->sqlite_wrapper);
  }

  std::cout << "done." << std::endl;

  benchmark_runner->run();
}
