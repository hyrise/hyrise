#include <filesystem>
#include <string>
#include <unordered_map>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include "cxxopts.hpp"

#include "benchmark_runner.hpp"
#include "cli_config_parser.hpp"
#include "file_based_benchmark_item_runner.hpp"
#include "file_based_table_generator.hpp"
#include "hyrise.hpp"
#include "storage/constraints/constraint_utils.hpp"
#include "types.hpp"
#include "utils/performance_warning.hpp"
#include "utils/sqlite_add_indices.hpp"

/**
 * The Join Order Benchmark was introduced by Leis et al. "How good are query optimizers, really?". It runs on an IMDB
 * database from ~2013 that gets downloaded if necessary as part of running this benchmark. Its 113 queries are obtained
 * from the "third_party/join-order-benchmark" submodule. For an overview of the schema, see:
 * https://doi.org/10.1007/s00778-017-0480-7 (Leis et al. "Query optimization through the looking glass, and what we
 * found running the Join Order Benchmark" - 2.1 The IMDB data set, Fig. 2, p. 645.)
 */

using namespace hyrise;  // NOLINT(build/namespaces)

void add_key_constraints(std::unordered_map<std::string, BenchmarkTableInfo>& table_info_by_name) {
  // Get all tables.
  const auto& aka_name_table = table_info_by_name.at("aka_name").table;
  const auto& aka_title_table = table_info_by_name.at("aka_title").table;
  const auto& cast_info_table = table_info_by_name.at("cast_info").table;
  const auto& char_name_table = table_info_by_name.at("char_name").table;
  const auto& comp_cast_type_table = table_info_by_name.at("comp_cast_type").table;
  const auto& company_name_table = table_info_by_name.at("company_name").table;
  const auto& company_type_table = table_info_by_name.at("company_type").table;
  const auto& complete_cast_table = table_info_by_name.at("complete_cast").table;
  const auto& info_type_table = table_info_by_name.at("info_type").table;
  const auto& keyword_table = table_info_by_name.at("keyword").table;
  const auto& kind_type_table = table_info_by_name.at("kind_type").table;
  const auto& link_type_table = table_info_by_name.at("link_type").table;
  const auto& movie_companies_table = table_info_by_name.at("movie_companies").table;
  const auto& movie_info_table = table_info_by_name.at("movie_info").table;
  const auto& movie_info_idx_table = table_info_by_name.at("movie_info_idx").table;
  const auto& movie_keyword_table = table_info_by_name.at("movie_keyword").table;
  const auto& movie_link_table = table_info_by_name.at("movie_link").table;
  const auto& name_table = table_info_by_name.at("name").table;
  const auto& person_info_table = table_info_by_name.at("person_info").table;
  const auto& role_type_table = table_info_by_name.at("role_type").table;
  const auto& title_table = table_info_by_name.at("title").table;

  // Set constraints.

  // aka_name - 1 PK, 1 FK.
  primary_key_constraint(aka_name_table, {"id"});
  foreign_key_constraint(aka_name_table, {"person_id"}, name_table, {"id"});

  // aka_title - 1 PK, 1 FK.
  primary_key_constraint(aka_title_table, {"id"});
  foreign_key_constraint(aka_title_table, {"movie_id"}, title_table, {"id"});

  // cast_info - 1 PK, 4 FKs.
  primary_key_constraint(cast_info_table, {"id"});
  foreign_key_constraint(cast_info_table, {"movie_id"}, title_table, {"id"});
  foreign_key_constraint(cast_info_table, {"person_id"}, name_table, {"id"});
  foreign_key_constraint(cast_info_table, {"person_role_id"}, char_name_table, {"id"});
  foreign_key_constraint(cast_info_table, {"role_id"}, role_type_table, {"id"});

  // char_name - 1 PK.
  primary_key_constraint(char_name_table, {"id"});

  // comp_cast_type - 1 PK.
  primary_key_constraint(comp_cast_type_table, {"id"});

  // company_name - 1 PK.
  primary_key_constraint(company_name_table, {"id"});

  // company_type - 1 PK.
  primary_key_constraint(company_type_table, {"id"});

  // complete_cast - 1 PK, 3 FKs.
  primary_key_constraint(complete_cast_table, {"id"});
  foreign_key_constraint(complete_cast_table, {"subject_id"}, comp_cast_type_table, {"id"});
  foreign_key_constraint(complete_cast_table, {"status_id"}, comp_cast_type_table, {"id"});
  foreign_key_constraint(complete_cast_table, {"movie_id"}, title_table, {"id"});

  // info_type - 1 PK.
  primary_key_constraint(info_type_table, {"id"});

  // keyword - 1 PK.
  primary_key_constraint(keyword_table, {"id"});

  // kind_type - 1 PK.
  primary_key_constraint(kind_type_table, {"id"});

  // link_type - 1 PK.
  primary_key_constraint(link_type_table, {"id"});

  // movie_companies - 1 PK, 3 FKs.
  primary_key_constraint(movie_companies_table, {"id"});
  foreign_key_constraint(movie_companies_table, {"company_id"}, company_name_table, {"id"});
  foreign_key_constraint(movie_companies_table, {"movie_id"}, title_table, {"id"});
  foreign_key_constraint(movie_companies_table, {"company_type_id"}, company_type_table, {"id"});

  // movie_info - 1 PK, 2 FKs.
  primary_key_constraint(movie_info_table, {"id"});
  foreign_key_constraint(movie_info_table, {"movie_id"}, title_table, {"id"});
  foreign_key_constraint(movie_info_table, {"info_type_id"}, info_type_table, {"id"});

  // movie_info_idx - 1 PK, 2 FKs.
  primary_key_constraint(movie_info_idx_table, {"id"});
  foreign_key_constraint(movie_info_idx_table, {"movie_id"}, title_table, {"id"});
  foreign_key_constraint(movie_info_idx_table, {"info_type_id"}, info_type_table, {"id"});

  // movie_keyword - 1 PK, 2 FKs.
  primary_key_constraint(movie_keyword_table, {"id"});
  foreign_key_constraint(movie_keyword_table, {"movie_id"}, title_table, {"id"});
  foreign_key_constraint(movie_keyword_table, {"keyword_id"}, keyword_table, {"id"});

  // movie_link - 1 PK, 3 FKs.
  primary_key_constraint(movie_link_table, {"id"});
  foreign_key_constraint(movie_link_table, {"movie_id"}, title_table, {"id"});
  foreign_key_constraint(movie_link_table, {"linked_movie_id"}, title_table, {"id"});
  foreign_key_constraint(movie_link_table, {"link_type_id"}, link_type_table, {"id"});

  // name - 1 PK.
  primary_key_constraint(name_table, {"id"});

  // person_info - 1 PK, 2 FKs.
  primary_key_constraint(person_info_table, {"id"});
  foreign_key_constraint(person_info_table, {"person_id"}, name_table, {"id"});
  foreign_key_constraint(person_info_table, {"info_type_id"}, info_type_table, {"id"});

  // role_type - 1 PK.
  primary_key_constraint(role_type_table, {"id"});

  // title - 1 PK, 1 FK.
  primary_key_constraint(title_table, {"id"});
  foreign_key_constraint(title_table, {"kind_id"}, kind_type_table, {"id"});
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

  auto benchmark_config = std::shared_ptr<BenchmarkConfig>{};
  auto query_path = std::string{};
  auto table_path = std::string{};
  // Comma-separated query names or "all"
  auto queries_str = std::string{};

  // Parse command line args
  const auto cli_parse_result = cli_options.parse(argc, argv);

  if (CLIConfigParser::print_help_if_requested(cli_options, cli_parse_result)) {
    return 0;
  }

  query_path = cli_parse_result["query_path"].as<std::string>();
  table_path = cli_parse_result["table_path"].as<std::string>();
  queries_str = cli_parse_result["queries"].as<std::string>();

  benchmark_config = CLIConfigParser::parse_cli_options(cli_parse_result);

  // Check that the options "query_path" and "table_path" were specified
  if (query_path.empty() || table_path.empty()) {
    std::cerr << "Need to specify --query_path=path/to/queries and --table_path=path/to/table_files\n";
    std::cerr << cli_options.help({}) << '\n';
    return 1;
  }

  /**
   * Use a Python script to download and unzip the IMDB. We do this in Python and not in C++ because downloading and
   * unzipping is straight forward in Python (and we suspect in C++ it might be... cumbersome).
   */
  const auto setup_imdb_command = "python3 scripts/setup_imdb.py " + table_path;
  const auto setup_imdb_return_code = std::system(setup_imdb_command.c_str());
  Assert(setup_imdb_return_code == 0, "setup_imdb.py failed. Did you run the benchmark from the project root dir?");

  // The join-order-benchmark ships with these two .sql scripts, but we do not want to run them as part of the benchmark
  // as they do not contains actual queries
  const auto non_query_file_names = std::unordered_set<std::string>{"fkindexes.sql", "schema.sql"};

  std::cout << "- Benchmarking queries from " << query_path << '\n';
  std::cout << "- Running on tables from " << table_path << '\n';

  auto query_subset = std::optional<std::unordered_set<std::string>>{};
  if (queries_str == "all") {
    std::cout << "- Running all queries from specified path\n";
  } else {
    std::cout << "- Running subset of queries: " << queries_str << '\n';

    // "a, b, c, d" -> ["a", " b", " c", " d"]
    auto query_subset_untrimmed = std::vector<std::string>{};
    boost::algorithm::split(query_subset_untrimmed, queries_str, boost::is_any_of(","));

    // ["a", " b", " c", " d"] -> ["a", "b", "c", "d"]
    query_subset.emplace();
    for (const auto& query_name : query_subset_untrimmed) {
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

  std::cout << "done.\n";

  benchmark_runner->run();
}
