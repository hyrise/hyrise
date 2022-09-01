#include <filesystem>

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

/**
 * The Join Order Benchmark was introduced by Leis et al. "How good are query optimizers, really?".
 * It runs on an IMDB database from ~2013 that gets downloaded if necessary as part of running this benchmark.
 * Its 113 queries are obtained from the "third_party/join-order-benchmark" submodule
 */

using namespace hyrise;                // NOLINT
using namespace std::string_literals;  // NOLINT

/**
 * Each of the 21 JOB tables has one surrogate key. This function registers key constraints for all of them.
 */
void add_key_constraints(std::unordered_map<std::string, BenchmarkTableInfo>& table_info_by_name) {
  const auto& aka_name_table = table_info_by_name.at("aka_name").table;
  aka_name_table->add_soft_key_constraint({{aka_name_table->column_id_by_name("id")}, KeyConstraintType::PRIMARY_KEY});

  const auto& aka_title_table = table_info_by_name.at("aka_title").table;
  aka_title_table->add_soft_key_constraint(
      {{aka_title_table->column_id_by_name("id")}, KeyConstraintType::PRIMARY_KEY});

  const auto& cast_info_table = table_info_by_name.at("cast_info").table;
  cast_info_table->add_soft_key_constraint(
      {{cast_info_table->column_id_by_name("id")}, KeyConstraintType::PRIMARY_KEY});

  const auto& char_name_table = table_info_by_name.at("char_name").table;
  char_name_table->add_soft_key_constraint(
      {{char_name_table->column_id_by_name("id")}, KeyConstraintType::PRIMARY_KEY});

  const auto& comp_cast_type_table = table_info_by_name.at("comp_cast_type").table;
  comp_cast_type_table->add_soft_key_constraint(
      {{comp_cast_type_table->column_id_by_name("id")}, KeyConstraintType::PRIMARY_KEY});

  const auto& company_name_table = table_info_by_name.at("company_name").table;
  company_name_table->add_soft_key_constraint(
      {{company_name_table->column_id_by_name("id")}, KeyConstraintType::PRIMARY_KEY});

  const auto& company_type_table = table_info_by_name.at("company_type").table;
  company_type_table->add_soft_key_constraint(
      {{company_type_table->column_id_by_name("id")}, KeyConstraintType::PRIMARY_KEY});

  const auto& complete_cast_table = table_info_by_name.at("complete_cast").table;
  complete_cast_table->add_soft_key_constraint(
      {{complete_cast_table->column_id_by_name("id")}, KeyConstraintType::PRIMARY_KEY});

  const auto& info_type_table = table_info_by_name.at("info_type").table;
  info_type_table->add_soft_key_constraint(
      {{info_type_table->column_id_by_name("id")}, KeyConstraintType::PRIMARY_KEY});

  const auto& keyword_table = table_info_by_name.at("keyword").table;
  keyword_table->add_soft_key_constraint({{keyword_table->column_id_by_name("id")}, KeyConstraintType::PRIMARY_KEY});

  const auto& kind_type_table = table_info_by_name.at("kind_type").table;
  kind_type_table->add_soft_key_constraint(
      {{kind_type_table->column_id_by_name("id")}, KeyConstraintType::PRIMARY_KEY});

  const auto& link_type_table = table_info_by_name.at("link_type").table;
  link_type_table->add_soft_key_constraint(
      {{link_type_table->column_id_by_name("id")}, KeyConstraintType::PRIMARY_KEY});

  const auto& movie_companies_table = table_info_by_name.at("movie_companies").table;
  movie_companies_table->add_soft_key_constraint(
      {{movie_companies_table->column_id_by_name("id")}, KeyConstraintType::PRIMARY_KEY});

  const auto& movie_info_table = table_info_by_name.at("movie_info").table;
  movie_info_table->add_soft_key_constraint(
      {{movie_info_table->column_id_by_name("id")}, KeyConstraintType::PRIMARY_KEY});

  const auto& movie_info_idx_table = table_info_by_name.at("movie_info_idx").table;
  movie_info_idx_table->add_soft_key_constraint(
      {{movie_info_idx_table->column_id_by_name("id")}, KeyConstraintType::PRIMARY_KEY});

  const auto& movie_keyword_table = table_info_by_name.at("movie_keyword").table;
  movie_keyword_table->add_soft_key_constraint(
      {{movie_keyword_table->column_id_by_name("id")}, KeyConstraintType::PRIMARY_KEY});

  const auto& movie_link_table = table_info_by_name.at("movie_link").table;
  movie_link_table->add_soft_key_constraint(
      {{movie_link_table->column_id_by_name("id")}, KeyConstraintType::PRIMARY_KEY});

  const auto& name_table = table_info_by_name.at("name").table;
  name_table->add_soft_key_constraint({{name_table->column_id_by_name("id")}, KeyConstraintType::PRIMARY_KEY});

  const auto& person_info_table = table_info_by_name.at("person_info").table;
  person_info_table->add_soft_key_constraint(
      {{person_info_table->column_id_by_name("id")}, KeyConstraintType::PRIMARY_KEY});

  const auto& role_type_table = table_info_by_name.at("role_type").table;
  role_type_table->add_soft_key_constraint(
      {{role_type_table->column_id_by_name("id")}, KeyConstraintType::PRIMARY_KEY});

  const auto& title_table = table_info_by_name.at("title").table;
  title_table->add_soft_key_constraint({{title_table->column_id_by_name("id")}, KeyConstraintType::PRIMARY_KEY});
}

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

  // Parse command line args
  const auto cli_parse_result = cli_options.parse(argc, argv);

  if (CLIConfigParser::print_help_if_requested(cli_options, cli_parse_result)) {
    return 0;
  }

  query_path = cli_parse_result["query_path"].as<std::string>();
  table_path = cli_parse_result["table_path"].as<std::string>();
  queries_str = cli_parse_result["queries"].as<std::string>();

  benchmark_config = std::make_shared<BenchmarkConfig>(CLIConfigParser::parse_cli_options(cli_parse_result));

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
  table_generator->set_add_constraints_callback(add_key_constraints);
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
