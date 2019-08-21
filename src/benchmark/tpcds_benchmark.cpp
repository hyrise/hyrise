#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include <chrono>
#include <fstream>
#include <iostream>
#include <random>
#include <string>

#include "SQLParser.h"
#include "SQLParserResult.h"
#include "benchmark_runner.hpp"
#include "cli_config_parser.hpp"
#include "cxxopts.hpp"
#include "file_based_benchmark_item_runner.hpp"
#include "file_based_table_generator.hpp"
#include "json.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/chunk_encoder.hpp"
#include "tpcds/tpcds_table_generator.hpp"
#include "utils/assert.hpp"
#include "utils/sqlite_add_indices.hpp"
#include "visualization/lqp_visualizer.hpp"
#include "visualization/pqp_visualizer.hpp"

using namespace opossum;  // NOLINT

namespace {
const std::unordered_set<std::string> filename_blacklist() {
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

int main(int argc, char* argv[]) {
  auto cli_options = opossum::BenchmarkRunner::get_basic_cli_options("TPC-DS Benchmark");

  // clang-format off
  cli_options.add_options()
    ("s,scale", "Database scale factor (1 ~ 1GB)", cxxopts::value<int32_t>()->default_value("1"));
  // clang-format on

  auto config = std::shared_ptr<opossum::BenchmarkConfig>{};
  auto scale_factor = int32_t{};
  if (opossum::CLIConfigParser::cli_has_json_config(argc, argv)) {
    // JSON config file was passed in
    const auto json_config = opossum::CLIConfigParser::parse_json_config_file(argv[1]);
    scale_factor = json_config.value("scale", 1);
    config = std::make_shared<opossum::BenchmarkConfig>(
        opossum::CLIConfigParser::parse_basic_options_json_config(json_config));
  } else {
    // Parse regular command line args
    const auto cli_parse_result = cli_options.parse(argc, argv);

    if (CLIConfigParser::print_help_if_requested(cli_options, cli_parse_result)) {
      return 0;
    }
    scale_factor = cli_parse_result["scale"].as<int32_t>();

    config =
        std::make_shared<opossum::BenchmarkConfig>(opossum::CLIConfigParser::parse_basic_cli_options(cli_parse_result));
  }

  const auto valid_scale_factors = std::array{1, 1000, 3000, 10000, 30000, 100000};

  const auto& find_result = std::find(valid_scale_factors.begin(), valid_scale_factors.end(), scale_factor);
  Assert(find_result != valid_scale_factors.end(),
         "TPC-DS benchmark only supports scale factor 1 (qualification only), 1000, 3000, 10000, 30000 and 100000.");

  std::cout << "- TPC-DS scale factor is " << scale_factor << std::endl;

  std::string query_path = "resources/benchmark/tpcds/tpcds-result-reproduction/query_qualification";

  Assert(std::filesystem::is_directory(query_path), "Query path (" + query_path + ") has to be a directory.");
  Assert(std::filesystem::exists(std::filesystem::path{query_path + "/01.sql"}), "Queries have to be available.");

  auto query_generator = std::make_unique<FileBasedBenchmarkItemRunner>(config, query_path, filename_blacklist());
  if (config->verify) {
    query_generator->load_dedicated_expected_results(
        std::filesystem::path{"resources/benchmark/tpcds/tpcds-result-reproduction/answer_sets_tbl"});
  }
  auto table_generator = std::make_unique<TpcdsTableGenerator>(scale_factor, config);
  auto benchmark_runner = BenchmarkRunner{*config, std::move(query_generator), std::move(table_generator),
                                          opossum::BenchmarkRunner::create_context(*config)};
  if (config->verify) {
    add_indices_to_sqlite("resources/benchmark/tpcds/schema.sql", "resources/benchmark/tpcds/create_indices.sql",
                          benchmark_runner.sqlite_wrapper);
  }
  std::cout << "done." << std::endl;

  benchmark_runner.run();
}
