#include <filesystem>
#include <fstream>

#include <boost/algorithm/string.hpp>
#include <cxxopts.hpp>

#include "benchmark_runner.hpp"
#include "cli_config_parser.hpp"
#include "file_based_benchmark_item_runner.hpp"
#include "file_based_table_generator.hpp"
#include "hyrise.hpp"
#include "import_export/csv/csv_writer.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "types.hpp"
#include "utils/performance_warning.hpp"
#include "utils/sqlite_add_indices.hpp"

/**
 * The Public BI Benchmark was introduced by Vogelsang et al. "Get Real: How Benchmarks Fail to Represent the Real World".
 * It runs on Tableau workbooks from ~2018 that get downloaded if necessary as part of running this benchmark.
 * Its 646 queries for 46 benchmarks are obtained from the "third_party/public_bi_benchmark" submodule
 */

using namespace opossum;               // NOLINT
using namespace std::string_literals;  // NOLINT

namespace {
const std::unordered_set<std::string> filename_blacklist() {
  auto filename_blacklist = std::unordered_set<std::string>{};
  const auto blacklist_file_path = "resources/benchmark/public_bi/query_blacklist.cfg";
  std::ifstream blacklist_file(blacklist_file_path);

  if (!blacklist_file) {
    std::cerr << "Cannot open the blacklist file: " << blacklist_file_path << "\n";
  } else {
    std::string filename;
    while (std::getline(blacklist_file, filename)) {
      if (filename.size() > 0) {
        filename_blacklist.emplace(filename);
      }
    }
    blacklist_file.close();
  }
  return filename_blacklist;
}

template <typename Functor>
std::unordered_set<std::string> files_in_directory(const std::string& directory, Functor fn) {
  auto files = std::unordered_set<std::string>{};
  for (const auto& directory_entry : std::filesystem::directory_iterator(directory)) {
    if (!std::filesystem::is_regular_file(directory_entry)) {
      continue;
    }
    const auto file_name = std::string{directory_entry.path().stem()};
    files.emplace(fn(file_name));
  }
  return files;
}
}  // namespace

int main(int argc, char* argv[]) {
  auto cli_options = BenchmarkRunner::get_basic_cli_options("Hyrise Public BI Benchmark");

  const auto DEFAULT_DATA_DIRECTORY = "public_bi_data";
  const auto DEFAULT_REPO_DIRECTORY = "third_party/public_bi_benchmark";

  // clang-format off
  cli_options.add_options()
  ("data_directory", "Directory containing the tables and queries. Generated if necessary", cxxopts::value<std::string>()->default_value(DEFAULT_DATA_DIRECTORY)) // NOLINT
  ("repo_directory", "Root directory of the Public BI Benchmark repository", cxxopts::value<std::string>()->default_value(DEFAULT_REPO_DIRECTORY)) // NOLINT
  ("b,benchmarks", "Subset of benchmarks to run as a comma separated list", cxxopts::value<std::string>()->default_value("all")) // NOLINT
  ("s,skip_benchmarks", "Subset of benchmarks to skip as a comma separated list", cxxopts::value<std::string>()->default_value("")) // NOLINT
  ("run_together", "Load all datasets at once and run the queries in one benchmark execution", cxxopts::value<bool>()->default_value("false")); // NOLINT
  // clang-format on

  // Parse command line args
  const auto cli_parse_result = cli_options.parse(argc, argv);
  if (CLIConfigParser::print_help_if_requested(cli_options, cli_parse_result)) return 0;

  const auto repo_dir = cli_parse_result["repo_directory"].as<std::string>();
  const auto data_dir = cli_parse_result["data_directory"].as<std::string>();
  const auto run_together = cli_parse_result["run_together"].as<bool>();

  // Comma-separated query names or "all"
  const auto benchmarks_str = cli_parse_result["benchmarks"].as<std::string>();
  const auto skip_str = cli_parse_result["skip_benchmarks"].as<std::string>();

  auto benchmark_config = std::make_shared<BenchmarkConfig>(CLIConfigParser::parse_cli_options(cli_parse_result));

  // Check that the options "repo_directory" and "data_directory" were specified
  if (repo_dir.empty() || data_dir.empty()) {
    std::cerr << "Need to specify --repo_directory=path/to/queries and --data_directory=path/to/table_files"
              << std::endl;
    std::cerr << cli_options.help({}) << std::endl;
    return 1;
  }

  /**
   * Use a Python script to download and unpack the tables. We do this in Python and not in C++ because downloading and
   * unpacking is straight forward in Python (and we suspect in C++ it might be... cumbersome).
   */
  const auto setup_public_bi_command = "python3 scripts/setup_public_bi.py " + repo_dir + " " + data_dir;
  const auto setup_public_bi_return_code = system(setup_public_bi_command.c_str());
  Assert(setup_public_bi_return_code == 0,
         "setup_public_bi.py failed. Did you run the benchmark from the project root dir?");

  std::vector<std::string> available_benchmarks;

  for (const auto& directory_entry : std::filesystem::directory_iterator(repo_dir + "/benchmark")) {
    if (!std::filesystem::is_regular_file(directory_entry)) {
      available_benchmarks.emplace_back(std::string{directory_entry.path().stem()});
    }
  }
  std::sort(available_benchmarks.begin(), available_benchmarks.end());
  auto queries_per_benchmark = std::unordered_map<std::string, std::unordered_set<std::string>>{};
  auto tables_per_benchmark = std::unordered_map<std::string, std::unordered_set<std::string>>{};

  std::cout << "- Generating table meta information if necessary" << std::endl;
  const auto table_suffix = std::string{".table"};
  const auto table_suffix_size = table_suffix.size();
  for (const auto& benchmark : available_benchmarks) {
    const auto table_path = repo_dir + "/benchmark/" + benchmark + "/tables";
    const auto query_path = repo_dir + "/benchmark/" + benchmark + "/queries";
    tables_per_benchmark[benchmark] = files_in_directory(table_path, [&table_suffix_size](const auto& file_name) {
      return file_name.substr(0, file_name.size() - table_suffix_size);
    });
    queries_per_benchmark[benchmark] =
        files_in_directory(query_path, [&benchmark](const auto& file_name) { return benchmark + "." + file_name; });

    for (const auto& table_name : tables_per_benchmark[benchmark]) {
      const auto table_meta_path =
          data_dir + "/tables/" + std::string{table_name} + ".csv" + CsvMeta::META_FILE_EXTENSION;
      std::ifstream file(table_meta_path);
      const auto exists = file.is_open();
      file.close();

      if (exists) {
        continue;
      }

      // Read and execute CREATE TABLE statement, export CSV meta file
      const auto create_table_path = table_path + "/" + table_name + table_suffix + ".sql";
      std::ifstream definition_file(create_table_path);
      Assert(definition_file.is_open(), "Did not find table definition for " + table_name);

      std::ostringstream definition_stream;
      definition_stream << definition_file.rdbuf();
      auto create_table_statement_string = definition_stream.str();
      definition_file.close();

      // Replace unsupported data types with String
      const auto replace_keywords =
          std::vector<std::pair<std::string, std::string>>{{"timestamp", "text"}, {"boolean", "text"}};
      for (const auto& [keyword, replacement] : replace_keywords) {
        while (true) {
          const auto keyword_position = create_table_statement_string.find(keyword);
          if (keyword_position == std::string::npos) {
            break;
          }
          create_table_statement_string.replace(keyword_position, keyword.size(), replacement);
        }
      }

      // Execute statement to get table with corresponding column definitions
      const auto create_table_node = SQLPipelineBuilder{create_table_statement_string}
                                         .disable_mvcc()
                                         .create_pipeline()
                                         .get_unoptimized_logical_plans()
                                         .at(0);
      const auto& static_table_node = static_cast<StaticTableNode&>(*create_table_node->left_input());

      // Write CSV meta
      CsvMeta csv_meta{};
      csv_meta.config.separator = '|';
      csv_meta.config.null_handling = NullHandling::NullStringAsNull;
      csv_meta.config.quote = '\0';
      csv_meta.config.escape = '\\';
      csv_meta.config.rfc_mode = false;
      CsvWriter::generate_meta_info_file(*static_table_node.table, table_meta_path, csv_meta);
    }
  }

  const auto query_path = data_dir + "/queries";
  const auto table_path = data_dir + "/tables";
  std::cout << "- Benchmarking queries from " << query_path << std::endl;
  std::cout << "- Running on tables from " << table_path << std::endl;

  // Determine subset of benchmarks/queries if requested
  std::optional<std::unordered_map<std::string, std::unordered_set<std::string>>> query_subset;
  std::vector<std::string> benchmarks_to_run;
  if (benchmarks_str == "all" && skip_str.empty()) {
    std::cout << "- Running all queries from specified path" << std::endl;
    benchmarks_to_run = available_benchmarks;
  } else {
    const auto subset = skip_str.empty() ? "" : " w/o " + skip_str;
    std::cout << "- Running subset of benchmarks: " << benchmarks_str << subset << std::endl;

    const auto split_benchmark_list = [&queries_per_benchmark, &available_benchmarks](const auto& benchmark_list_str) {
      auto benchmark_list = std::vector<std::string>{};
      boost::algorithm::split(benchmark_list, benchmark_list_str, boost::is_any_of(","));

      for (const auto& benchmark_name : benchmark_list) {
        AssertInput(queries_per_benchmark.contains(benchmark_name),
                    "Unknown benchmark '" + benchmark_name + "'. Available choices: all / " +
                        boost::algorithm::join(available_benchmarks, ","));
      }
      return benchmark_list;
    };

    auto excluded_benchmarks = std::unordered_set<std::string>{};
    if (!skip_str.empty()) {
      const auto& skipped_benchmarks = split_benchmark_list(skip_str);
      excluded_benchmarks.insert(skipped_benchmarks.begin(), skipped_benchmarks.end());
    }

    const auto& selected_benchmarks =
        benchmarks_str == "all" ? available_benchmarks : split_benchmark_list(benchmarks_str);
    for (const auto& benchmark : selected_benchmarks) {
      if (!excluded_benchmarks.contains(benchmark)) {
        benchmarks_to_run.emplace_back(benchmark);
      }
    }

    query_subset.emplace();
    for (const auto& benchmark : benchmarks_to_run) {
      (*query_subset)[benchmark].insert(queries_per_benchmark[benchmark].begin(),
                                        queries_per_benchmark[benchmark].end());
    }
  }

  const auto blacklist = filename_blacklist();
  if (run_together) {
    std::cout << "- Run all queries together" << std::endl;

    auto benchmark_queries = std::optional<std::unordered_set<std::string>>{};
    if (query_subset) {
      benchmark_queries.emplace();
      for (const auto& [_, queries] : *query_subset) {
        benchmark_queries->insert(queries.begin(), queries.end());
      }
    }

    auto tables_to_load = std::unordered_set<std::string>{};
    for (const auto& benchmark : benchmarks_to_run) {
      tables_to_load.insert(tables_per_benchmark[benchmark].begin(), tables_per_benchmark[benchmark].end());
    }

    // Run the benchmark
    auto context = BenchmarkRunner::create_context(*benchmark_config);
    auto table_generator = std::make_unique<FileBasedTableGenerator>(benchmark_config, table_path, tables_to_load);
    auto benchmark_item_runner =
        std::make_unique<FileBasedBenchmarkItemRunner>(benchmark_config, query_path, blacklist, benchmark_queries);

    if (benchmark_item_runner->items().empty()) {
      std::cout << "- No items to run.\n";
      return 1;
    }

    auto benchmark_runner = std::make_shared<BenchmarkRunner>(*benchmark_config, std::move(benchmark_item_runner),
                                                              std::move(table_generator), context);
    Hyrise::get().benchmark_runner = benchmark_runner;
    benchmark_runner->run();

    std::cout << "done." << std::endl;
  } else {
    std::cout << "- Run benchmarks separately" << std::endl;
    for (const auto& benchmark : benchmarks_to_run) {
      const auto& benchmark_queries = query_subset ? (*query_subset)[benchmark] : queries_per_benchmark[benchmark];

      std::cout << "- " << benchmark << std::endl;
      auto context = BenchmarkRunner::create_context(*benchmark_config);
      auto table_generator =
          std::make_unique<FileBasedTableGenerator>(benchmark_config, table_path, tables_per_benchmark[benchmark]);
      auto benchmark_item_runner =
          std::make_unique<FileBasedBenchmarkItemRunner>(benchmark_config, query_path, blacklist, benchmark_queries);

      if (benchmark_item_runner->items().empty()) {
        std::cout << "- No items to run.\n";
        continue;
      }

      auto benchmark_runner = std::make_shared<BenchmarkRunner>(*benchmark_config, std::move(benchmark_item_runner),
                                                                std::move(table_generator), context);
      Hyrise::get().benchmark_runner = benchmark_runner;
      benchmark_runner->run();
    }
    std::cout << "done." << std::endl;
  }
}
