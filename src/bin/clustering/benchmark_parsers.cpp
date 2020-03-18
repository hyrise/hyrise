#include "benchmark_parsers.hpp"

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <fstream>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
#include "cli_config_parser.hpp"
#include "cxxopts.hpp"
#include "file_based_benchmark_item_runner.hpp"
#include "file_based_table_generator.hpp"
#include "hyrise.hpp"
#include "tpcds/tpcds_table_generator.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "tpch/tpch_queries.hpp"
#include "utils/sqlite_add_indices.hpp"


using namespace std::string_literals;  // NOLINT

namespace {
const std::unordered_set<std::string> _tpcds_filename_blacklist() {
  auto filename_blacklist = std::unordered_set<std::string>{};
  const auto blacklist_file_path = "resources/benchmark/tpcds/query_blacklist.cfg";
  std::ifstream blacklist_file(blacklist_file_path);

  if (!blacklist_file) {
    std::cerr << "Cannot open the blacklist file: " << blacklist_file_path << "\n";
  } else {
    std::string filename;
    while (std::getline(blacklist_file, filename)) {
      if (filename.size() > 0 && filename.at(0) != '#') {
        filename_blacklist.emplace(filename);
      }
    }
    blacklist_file.close();
  }
  return filename_blacklist;
}
}  // namespace


namespace opossum {

std::shared_ptr<BenchmarkRunner> parse_tpcds_args(int argc, char* argv[]) {
  auto cli_options = opossum::BenchmarkRunner::get_basic_cli_options("TPC-DS Benchmark");

  // clang-format off
  cli_options.add_options()
    ("s,scale", "Database scale factor (1 ~ 1GB)", cxxopts::value<int32_t>()->default_value("1"));
  // clang-format on

  auto config = std::shared_ptr<opossum::BenchmarkConfig>{};
  auto scale_factor = int32_t{};
  
  // Parse regular command line args - json config file is not supported, at least for now
  const auto cli_parse_result = cli_options.parse(argc, argv);

  if (CLIConfigParser::print_help_if_requested(cli_options, cli_parse_result)) {
    std::exit(0);
  }
  scale_factor = cli_parse_result["scale"].as<int32_t>();

  config =
      std::make_shared<opossum::BenchmarkConfig>(opossum::CLIConfigParser::parse_basic_cli_options(cli_parse_result));

  // no matter what the user says, never cache binary tables
  config->cache_binary_tables = false;

  const auto valid_scale_factors = std::array{1, 1000, 3000, 10000, 30000, 100000};

  const auto& find_result = std::find(valid_scale_factors.begin(), valid_scale_factors.end(), scale_factor);
  Assert(find_result != valid_scale_factors.end(),
         "TPC-DS benchmark only supports scale factor 1 (qualification only), 1000, 3000, 10000, 30000 and 100000.");

  std::cout << "- TPC-DS scale factor is " << scale_factor << std::endl;

  std::string query_path = "resources/benchmark/tpcds/tpcds-result-reproduction/query_qualification";

  Assert(std::filesystem::is_directory(query_path), "Query path (" + query_path + ") has to be a directory.");
  Assert(std::filesystem::exists(std::filesystem::path{query_path + "/01.sql"}), "Queries have to be available.");

  auto query_generator = std::make_unique<FileBasedBenchmarkItemRunner>(config, query_path, _tpcds_filename_blacklist());
  if (config->verify) {
    query_generator->load_dedicated_expected_results(
        std::filesystem::path{"resources/benchmark/tpcds/tpcds-result-reproduction/answer_sets_tbl"});
  }
  auto table_generator = std::make_unique<TpcdsTableGenerator>(scale_factor, config);
  auto benchmark_runner = std::make_shared<BenchmarkRunner>(*config, std::move(query_generator), std::move(table_generator),
                                          opossum::BenchmarkRunner::create_context(*config));
  if (config->verify) {
    add_indices_to_sqlite("resources/benchmark/tpcds/schema.sql", "resources/benchmark/tpcds/create_indices.sql",
                          benchmark_runner->sqlite_wrapper);
  }
  std::cout << "done." << std::endl;

  return benchmark_runner;
}

std::shared_ptr<BenchmarkRunner> parse_tpch_args(int argc, char* argv[]) {
  auto cli_options = BenchmarkRunner::get_basic_cli_options("TPC-H Benchmark");

  // clang-format off
  cli_options.add_options()
    ("s,scale", "Database scale factor (1.0 ~ 1GB)", cxxopts::value<float>()->default_value("1"))
    ("q,queries", "Specify queries to run (comma-separated query ids, e.g. \"--queries 1,3,19\"), default is all", cxxopts::value<std::string>()) // NOLINT
    ("use_prepared_statements", "Use prepared statements instead of random SQL strings", cxxopts::value<bool>()->default_value("false")); // NOLINT
  // clang-format on

  std::shared_ptr<BenchmarkConfig> config;
  std::string comma_separated_queries;
  float scale_factor;
  bool use_prepared_statements;

  
  // Parse regular command line args - json config file is not supported, at least for now
  const auto cli_parse_result = cli_options.parse(argc, argv);

  if (CLIConfigParser::print_help_if_requested(cli_options, cli_parse_result)) std::exit(0);

  if (cli_parse_result.count("queries")) {
    comma_separated_queries = cli_parse_result["queries"].as<std::string>();
  }

  scale_factor = cli_parse_result["scale"].as<float>();

  config = std::make_shared<BenchmarkConfig>(CLIConfigParser::parse_basic_cli_options(cli_parse_result));

  use_prepared_statements = cli_parse_result["use_prepared_statements"].as<bool>();
  
  // no matter what the user says, never cache binary tables
  config->cache_binary_tables = false;

  std::vector<BenchmarkItemID> item_ids;

  // Build list of query ids to be benchmarked and display it
  if (comma_separated_queries.empty()) {
    std::transform(tpch_queries.begin(), tpch_queries.end(), std::back_inserter(item_ids),
                   [](auto& pair) { return BenchmarkItemID{pair.first - 1}; });
  } else {
    // Split the input into query ids, ignoring leading, trailing, or duplicate commas
    auto item_ids_str = std::vector<std::string>();
    boost::trim_if(comma_separated_queries, boost::is_any_of(","));
    boost::split(item_ids_str, comma_separated_queries, boost::is_any_of(","), boost::token_compress_on);
    std::transform(item_ids_str.begin(), item_ids_str.end(), std::back_inserter(item_ids), [](const auto& item_id_str) {
      const auto item_id =
          BenchmarkItemID{boost::lexical_cast<BenchmarkItemID::base_type, std::string>(item_id_str) - 1};
      DebugAssert(item_id < 22, "There are only 22 TPC-H queries");
      return item_id;
    });
  }

  std::cout << "- Benchmarking Queries: [ ";
  auto printable_item_ids = std::vector<std::string>();
  std::for_each(item_ids.begin(), item_ids.end(),
                [&printable_item_ids](auto& id) { printable_item_ids.push_back(std::to_string(id + 1)); });
  std::cout << boost::algorithm::join(printable_item_ids, ", ") << " ]" << std::endl;

  auto context = BenchmarkRunner::create_context(*config);

  Assert(!use_prepared_statements || !config->verify, "SQLite validation does not work with prepared statements");

  if (config->verify) {
    // Hack: We cannot verify TPC-H Q15, thus we remove it from the list of queries
    auto it = std::remove(item_ids.begin(), item_ids.end(), 15 - 1);
    if (it != item_ids.end()) {
      // The problem is that the last part of the query, "DROP VIEW", does not return a table. Since we also have
      // the TPC-H test against a known-to-be-good table, we do not want the additional complexity for handling this
      // in the BenchmarkRunner.
      std::cout << "- Skipping Query 15 because it cannot easily be verified" << std::endl;
      item_ids.erase(it, item_ids.end());
    }
  }

  std::cout << "- TPCH scale factor is " << scale_factor << std::endl;
  std::cout << "- Using prepared statements: " << (use_prepared_statements ? "yes" : "no") << std::endl;

  // Add TPCH-specific information
  context.emplace("scale_factor", scale_factor);
  context.emplace("use_prepared_statements", use_prepared_statements);

  auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(config, use_prepared_statements, scale_factor, item_ids);
  auto benchmark_runner = std::make_shared<BenchmarkRunner>(
      *config, std::move(item_runner), std::make_unique<TPCHTableGenerator>(scale_factor, config), context);
  Hyrise::get().benchmark_runner = benchmark_runner;

  if (config->verify) {
    add_indices_to_sqlite("resources/benchmark/tpch/schema.sql", "resources/benchmark/tpch/indices.sql",
                          benchmark_runner->sqlite_wrapper);
  }

  return benchmark_runner;
}


std::shared_ptr<BenchmarkRunner> parse_job_args(int argc, char* argv[]) {
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

  // Parse regular command line args - json config is not supported, at least for now
  const auto cli_parse_result = cli_options.parse(argc, argv);

  if (CLIConfigParser::print_help_if_requested(cli_options, cli_parse_result)) std::exit(0);

  query_path = cli_parse_result["query_path"].as<std::string>();
  table_path = cli_parse_result["table_path"].as<std::string>();
  queries_str = cli_parse_result["queries"].as<std::string>();

  benchmark_config = std::make_shared<BenchmarkConfig>(CLIConfigParser::parse_basic_cli_options(cli_parse_result));
   // no matter what the user says, never cache binary tables
  benchmark_config->cache_binary_tables = false;

  // Check that the options "query_path" and "table_path" were specified
  if (query_path.empty() || table_path.empty()) {
    std::cerr << "Need to specify --query_path=path/to/queries and --table_path=path/to/table_files" << std::endl;
    std::cerr << cli_options.help({}) << std::endl;
    std::exit(1);
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
    std::exit(1);
  }

  auto benchmark_runner = std::make_shared<BenchmarkRunner>(*benchmark_config, std::move(benchmark_item_runner),
                                                            std::move(table_generator), context);
  Hyrise::get().benchmark_runner = benchmark_runner;

  if (benchmark_config->verify) {
    add_indices_to_sqlite(query_path + "/schema.sql", query_path + "/fkindexes.sql", benchmark_runner->sqlite_wrapper);
  }

  std::cout << "done." << std::endl;

  return benchmark_runner;
}

} // namespace opossum