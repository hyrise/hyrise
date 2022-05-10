#include <filesystem>
#include <fstream>

#include <boost/algorithm/string.hpp>
#include <cxxopts.hpp>

#include "benchmark_runner.hpp"
#include "cli_config_parser.hpp"
#include "file_based_benchmark_item_runner.hpp"
#include "file_based_table_generator.hpp"
#include "hyrise.hpp"
#include "types.hpp"
#include "utils/performance_warning.hpp"
#include "utils/sqlite_add_indices.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "import_export/csv/csv_writer.hpp"
#include "logical_query_plan/static_table_node.hpp"


/**
 * The Join Order Benchmark was introduced by Leis et al. "How good are query optimizers, really?".
 * It runs on an IMDB database from ~2013 that gets downloaded if necessary as part of running this benchmark.
 * Its 113 queries are obtained from the "third_party/join-order-benchmark" submodule
 */

using namespace opossum;               // NOLINT
using namespace std::string_literals;  // NOLINT

namespace {
const std::unordered_map<std::string, std::unordered_set<std::string>> filename_blacklist() {
  auto filename_blacklist = std::unordered_map<std::string, std::unordered_set<std::string>> {};
  const auto blacklist_file_path = "resources/benchmark/public_bi/query_blacklist.cfg";
  std::ifstream blacklist_file(blacklist_file_path);

  if (!blacklist_file) {
    std::cerr << "Cannot open the blacklist file: " << blacklist_file_path << "\n";
  } else {
    std::string filename;
    while (std::getline(blacklist_file, filename)) {
      if (filename.size() > 0 && filename.at(0) != '#') {
        const auto benchmark_length = filename.find_first_of(".");
        const auto benchmark = filename.substr(0, benchmark_length);
        filename_blacklist[benchmark].emplace(filename);
      }
    }
    blacklist_file.close();
  }
  for (const auto& [b, qs] : filename_blacklist) {
    std::cout << b << ": " << boost::algorithm::join(qs, ", ") << std::endl;
  }
  return filename_blacklist;
}

/*
template <typename ContainerType>
const ContainerType<std::string> list_directory(const std::string& path, const bool list_directories) {
  return ContainerType<std::string>{};
}
*/
}  // namespace

int main(int argc, char* argv[]) {
  auto cli_options = BenchmarkRunner::get_basic_cli_options("Hyrise Public BI Benchmark");

  const auto DEFAULT_DATA_DIRECTORY = "public_bi_data";
  const auto DEFAULT_REPO_DIRECTORY = "third_party/public_bi_benchmark";

  // clang-format off
  cli_options.add_options()
  ("data_directory", "Directory containing the Tables as csv, tbl or binary files. CSV files require meta-files, see csv_meta.hpp or any *.csv.json file.", cxxopts::value<std::string>()->default_value(DEFAULT_DATA_DIRECTORY)) // NOLINT
  ("repo_directory", "Root directory of the Public BI Benchmark repository", cxxopts::value<std::string>()->default_value(DEFAULT_REPO_DIRECTORY)) // NOLINT
  ("b,benchmarks", "Subset of benchmarks to run as a comma separated list", cxxopts::value<std::string>()->default_value("all")) // NOLINT
  ("d, dont_download", "Do not download the benchmark tables", cxxopts::value<bool>()->default_value("false"))
  ("run_together", "Load all datasets together and run the queries in one execution", cxxopts::value<bool>()->default_value("false"));
  // clang-format on

  std::shared_ptr<BenchmarkConfig> benchmark_config;
  std::string repo_dir;
  std::string data_dir;
  bool skip_download;
  bool run_together;
  // Comma-separated query names or "all"
  std::string benchmarks_str;

  // Parse command line args
  const auto cli_parse_result = cli_options.parse(argc, argv);

  if (CLIConfigParser::print_help_if_requested(cli_options, cli_parse_result)) return 0;

  repo_dir = cli_parse_result["repo_directory"].as<std::string>();
  data_dir = cli_parse_result["data_directory"].as<std::string>();
  benchmarks_str = cli_parse_result["benchmarks"].as<std::string>();
  skip_download = cli_parse_result["dont_download"].as<bool>();
  run_together = cli_parse_result["run_together"].as<bool>();


  benchmark_config = std::make_shared<BenchmarkConfig>(CLIConfigParser::parse_cli_options(cli_parse_result));

  // Check that the options "query_path" and "table_path" were specified
  if (repo_dir.empty() || data_dir.empty()) {
    std::cerr << "Need to specify --repo_directory=path/to/queries and --data_directory=path/to/table_files" << std::endl;
    std::cerr << cli_options.help({}) << std::endl;
    return 1;
  }


  if (!skip_download) {
    /**
     * Use a Python script to download and unzip the IMDB. We do this in Python and not in C++ because downloading and
     * unzipping is straight forward in Python (and we suspect in C++ it might be... cumbersome).
     */
    const auto setup_public_bi_command = "python3 scripts/setup_public_bi.py "s + data_dir + " -b " + repo_dir;
    const auto setup_public_bi_return_code = system(setup_public_bi_command.c_str());
    Assert(setup_public_bi_return_code == 0, "setup_public_bi.py failed. Did you run the benchmark from the project root dir?");
  }

  std::vector<std::string> benchmarks;

  for (const auto& directory_entry : std::filesystem::directory_iterator(repo_dir + "/benchmark")) {
    if (std::filesystem::is_regular_file(directory_entry)) continue;

    benchmarks.emplace_back(std::string{directory_entry.path().stem()});
  }
  std::sort(benchmarks.begin(), benchmarks.end());
  std::unordered_map<std::string, std::vector<std::string>> queries_per_benchmark;
  std::unordered_map<std::string, std::unordered_set<std::string>> tables_per_benchmark;

  std::cout << "- Generating table meta information if necessary" << std::endl;
  const auto suffix = std::string{".table"};
  const auto suffix_size = suffix.size();
  for (const auto& benchmark : benchmarks) {
    //std::cout << benchmark << std::endl;
    std::vector<std::string> tables;
    //std::cout << repo_dir + "/benchmark/" + benchmark + "/tables" << std::endl;

    const auto table_path = repo_dir + "/benchmark/" + benchmark + "/tables";
    const auto query_path = repo_dir + "/benchmark/" + benchmark + "/queries";

    for (const auto& directory_entry : std::filesystem::directory_iterator(table_path)) {
      if (!std::filesystem::is_regular_file(directory_entry)) continue;
      const auto identifier = std::string{directory_entry.path().stem()};
      const auto table_name = identifier.substr(0, identifier.size() - suffix_size);
      tables.emplace_back(table_name);
      tables_per_benchmark[benchmark].emplace(table_name);
    }

    for (const auto& directory_entry : std::filesystem::directory_iterator(query_path)) {
      if (!std::filesystem::is_regular_file(directory_entry)) continue;
      const auto identifier = std::string{directory_entry.path().stem()};
      queries_per_benchmark[benchmark].emplace_back(benchmark + "." + identifier);
    }


    std::sort(tables.begin(), tables.end());    for (const auto& table_name : tables) {
      //std::cout << "    " << table_name << std::endl;
      const auto table_meta_path = data_dir + "/tables/" + std::string{table_name} + ".csv" + CsvMeta::META_FILE_EXTENSION;
      //std::cout << table_meta_path << std::endl;
      std::ifstream file(table_meta_path);
      const auto exists = file.is_open();
      file.close();

      if (exists) {
        continue;
      }

      const auto create_table_path = repo_dir + "/benchmark/" + benchmark + "/tables/" + table_name + suffix + ".sql";
      std::ifstream definition_file(create_table_path);
      Assert(definition_file.is_open(), "Did not find table definition for " + table_name);

      std::ostringstream sstr;
      sstr << definition_file.rdbuf();
      auto create_table_statement_string = sstr.str();
      definition_file.close();

      const auto replace_keywords = std::vector<std::pair<std::string, std::string>>{{"timestamp", "text"}, {"boolean", "text"}};

      for (const auto& [kw, replacement] : replace_keywords) {
        while (true) {
          const auto p = create_table_statement_string.find(kw);
          if (p == std::string::npos) { break; }
          create_table_statement_string.replace(p, kw.size(), replacement);
        }
      }

      const auto create_table_node = SQLPipelineBuilder{create_table_statement_string}
        .disable_mvcc()
        .create_pipeline()
        .get_unoptimized_logical_plans().at(0);
      const auto& static_table_node = static_cast<StaticTableNode&>(*create_table_node->left_input());
      CsvMeta csv_meta{};
      csv_meta.config.separator = '|';
      csv_meta.config.null_handling = NullHandling::NullStringAsNull;
      CsvWriter::generate_meta_info_file(*static_table_node.table, table_meta_path, csv_meta);
    }
  }

  const auto blacklist_per_benchmark = filename_blacklist();
  auto blacklist =  std::unordered_set<std::string>{};
    for (const auto& [_, blacklist_queries] : blacklist_per_benchmark) {
      blacklist.insert(blacklist_queries.begin(), blacklist_queries.end());
    }


  // The join-order-benchmark ships with these two .sql scripts, but we do not want to run them as part of the benchmark
  // as they do not contains actual queries
  const auto non_query_file_names = std::unordered_set<std::string>{"fkindexes.sql", "schema.sql"};
  const auto query_path = data_dir + "/queries";
  const auto table_path = data_dir + "/tables";

  std::cout << "- Benchmarking queries from " << query_path << std::endl;
  std::cout << "- Running on tables from " << table_path << std::endl;

  std::optional<std::unordered_map<std::string, std::unordered_set<std::string>>> query_subset;
  std::vector<std::string> benchmarks_to_run;
  if (benchmarks_str == "all") {
    std::cout << "- Running all queries from specified path" << std::endl;
    benchmarks_to_run = benchmarks;
  } else {
    std::cout << "- Running subset of benchmarks: " << benchmarks_str << std::endl;

    // "a, b, c, d" -> ["a", " b", " c", " d"]
    auto benchmark_subset_untrimmed = std::vector<std::string>{};
    boost::algorithm::split(benchmark_subset_untrimmed, benchmarks_str, boost::is_any_of(","));

    // ["a", " b", " c", " d"] -> ["a", "b", "c", "d"]
    query_subset.emplace();
    for (auto& benchmark_name : benchmark_subset_untrimmed) {
      auto benchmark_trimmed = boost::trim_copy(benchmark_name);
      benchmarks_to_run.emplace_back(benchmark_trimmed);
      (*query_subset)[benchmark_trimmed].insert(queries_per_benchmark[benchmark_trimmed].begin(), queries_per_benchmark[benchmark_trimmed].end());
    }
  }



  if (run_together) {
    std::cout << "- Run all queries together" << std::endl;
    auto all_subset_queries = std::optional<std::unordered_set<std::string>>{};
    if (query_subset) {
      for (const auto& [_, queries] : *query_subset) {
        all_subset_queries->insert(queries.begin(), queries.end());
      }
    }

    // Run the benchmark
    auto context = BenchmarkRunner::create_context(*benchmark_config);
    auto table_generator = std::make_unique<FileBasedTableGenerator>(benchmark_config, table_path);
    auto benchmark_item_runner =
        std::make_unique<FileBasedBenchmarkItemRunner>(benchmark_config, query_path, blacklist, all_subset_queries);

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
  } else {
    std::cout << "- Run benchmarks separately" << std::endl;
    for (const auto& benchmark : benchmarks_to_run) {
      std::optional<std::unordered_set<std::string>> my_subset_queries;
      if (query_subset) {
        my_subset_queries = (*query_subset)[benchmark];
      }
      if (blacklist_per_benchmark.contains(benchmark)) {
        const auto num_queries = my_subset_queries ? my_subset_queries->size() : queries_per_benchmark[benchmark].size();
        if (num_queries == blacklist_per_benchmark.at(benchmark).size()) {
          std::cout << "- Skip " << benchmark << " (all queries blacklisted)" << std::endl;
          continue;
        }
      }
      std::cout << "- " << benchmark << std::endl;
      auto context = BenchmarkRunner::create_context(*benchmark_config);
      std::cout << boost::algorithm::join(tables_per_benchmark[benchmark], ", ") << std::endl;

      auto table_generator = std::make_unique<FileBasedTableGenerator>(benchmark_config, table_path, tables_per_benchmark[benchmark]);

      auto benchmark_item_runner =
        std::make_unique<FileBasedBenchmarkItemRunner>(benchmark_config, query_path, blacklist, my_subset_queries);


      if (benchmark_item_runner->items().empty()) {
        std::cout << "No items to run.\n";
        continue;
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
  }
}
