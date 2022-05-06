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

/**
 * Each of the 21 JOB tables has one surrogate key. This function registers key constraints for all of them.
 */
/*void add_key_constraints(std::unordered_map<std::string, BenchmarkTableInfo>& table_info_by_name) {
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
}*/

int main(int argc, char* argv[]) {
  auto cli_options = BenchmarkRunner::get_basic_cli_options("Hyrise Public BI Benchmark");

  const auto DEFAULT_TABLE_DIRECTORY = "public_bi_data";
  const auto DEFAULT_QUERY_DIRECTORY = "third_party/public_bi_benchmark";

  // clang-format off
  cli_options.add_options()
  ("table_directory", "Directory containing the Tables as csv, tbl or binary files. CSV files require meta-files, see csv_meta.hpp or any *.csv.json file.", cxxopts::value<std::string>()->default_value(DEFAULT_TABLE_DIRECTORY)) // NOLINT
  ("query_directory", "Directory containing the .sql files of the Join Order Benchmark", cxxopts::value<std::string>()->default_value(DEFAULT_QUERY_DIRECTORY)) // NOLINT
  ("b,benchmarks", "Subset of bnehcmarks to run as a comma separated list", cxxopts::value<std::string>()->default_value("all")); // NOLINT
  // clang-format on

  std::shared_ptr<BenchmarkConfig> benchmark_config;
  std::string query_dir;
  std::string table_dir;
  // Comma-separated query names or "all"
  std::string benchmarks_str;

  // Parse command line args
  const auto cli_parse_result = cli_options.parse(argc, argv);

  if (CLIConfigParser::print_help_if_requested(cli_options, cli_parse_result)) return 0;

  query_dir = cli_parse_result["query_directory"].as<std::string>();
  table_dir = cli_parse_result["table_directory"].as<std::string>();
  benchmarks_str = cli_parse_result["benchmarks"].as<std::string>();

  benchmark_config = std::make_shared<BenchmarkConfig>(CLIConfigParser::parse_cli_options(cli_parse_result));

  // Check that the options "query_path" and "table_path" were specified
  if (query_dir.empty() || table_dir.empty()) {
    std::cerr << "Need to specify --query_directory=path/to/queries and --table_directory=path/to/table_files" << std::endl;
    std::cerr << cli_options.help({}) << std::endl;
    return 1;
  }

  /**
   * Use a Python script to download and unzip the IMDB. We do this in Python and not in C++ because downloading and
   * unzipping is straight forward in Python (and we suspect in C++ it might be... cumbersome).
   */
  const auto setup_public_bi_command = "python3 scripts/setup_public_bi.py "s + table_dir + " -b " + query_dir;
  const auto setup_public_bi_return_code = system(setup_public_bi_command.c_str());
  Assert(setup_public_bi_return_code == 0, "setup_public_bi.py failed. Did you run the benchmark from the project root dir?");

  std::vector<std::filesystem::path> benchmarks;

  for (const auto& directory_entry : std::filesystem::directory_iterator(query_dir + "/benchmark")) {
    if (std::filesystem::is_regular_file(directory_entry)) continue;

    benchmarks.emplace_back(directory_entry.path().stem());
  }
  std::sort(benchmarks.begin(), benchmarks.end());

  std::cout << "- Generating table meta information if necessary" << std::endl;
  const auto suffix = std::string{".table"};
  const auto suffix_size = suffix.size();
  for (const auto& benchmark : benchmarks) {
    std::cout << benchmark << std::endl;
    std::vector<std::string> tables;
    std::cout << query_dir + "/benchmark/" + std::string{benchmark} + "/tables" << std::endl;

    const auto table_path = query_dir + "/benchmark/" + std::string{benchmark} + "/tables";

    for (const auto& directory_entry : std::filesystem::directory_iterator(table_path)) {
      if (!std::filesystem::is_regular_file(directory_entry)) continue;
      const auto identifier = std::string{directory_entry.path().stem()};
      tables.emplace_back(identifier.substr(0, identifier.size() - suffix_size));
      std::cout << identifier.substr(0, identifier.size() - suffix_size) << std::endl;
    }
    std::sort(tables.begin(), tables.end());
    for (const auto& table_name : tables) {
      std::cout << "    " << table_name << std::endl;
      const auto table_meta_path = table_dir + "/tables/" + std::string{table_name} + ".csv" + CsvMeta::META_FILE_EXTENSION;
      std::cout << table_meta_path << std::endl;
      std::ifstream file(table_meta_path);
      const auto exists = file.is_open();
      file.close();

      if (exists) {
        continue;
      }

      const auto create_table_path = query_dir + "/benchmark/" + std::string{benchmark} + "/tables/" + table_name + suffix + ".sql";
      std::ifstream definition_file(create_table_path);
      Assert(definition_file.is_open(), "Did not find table definition for " + table_name);

      std::ostringstream sstr;
      sstr << definition_file.rdbuf();
      auto create_table_statement_string = sstr.str();
      definition_file.close();

      const auto replace_keywords = std::vector<std::pair<std::string, std::string>>{{"timestamp", "text"}, {"boolean", "text"}, {"bigint", "long"}};

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
      const auto& static_table_node = static_cast<StaticTableNode&>(*table_node->left_input());
      CsvMeta meta{};
      meta.config.separator = '|';
      meta.config.null_handling = NullHandling::NullStringAsNull;
      CsvWriter::generate_meta_info_file(*static_table_node.table, table_meta_path, csv_meta);
      //std::cout << *create_table_node << std::endl;
    }

  }

  /*
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
  */
}
