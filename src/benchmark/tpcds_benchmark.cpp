#include <chrono>
#include <fstream>
#include <iostream>
#include <string>

#include "cxxopts.hpp"

#include "benchmark_runner.hpp"
#include "cli_config_parser.hpp"
#include "file_based_benchmark_item_runner.hpp"
#include "tpcds/tpcds_table_generator.hpp"
#include "utils/assert.hpp"
#include "utils/sqlite_add_indices.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

namespace {

const std::unordered_set<std::string> filename_blacklist() {
  auto filename_blacklist = std::unordered_set<std::string>{};
  const auto blacklist_file_path = "resources/benchmark/tpcds/query_blacklist.cfg";
  std::ifstream blacklist_file(blacklist_file_path);

  if (!blacklist_file) {
    std::cerr << "Cannot open the blacklist file: " << blacklist_file_path << "\n";
  } else {
    auto filename = std::string{};
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
  auto cli_options = BenchmarkRunner::get_basic_cli_options("TPC-DS Benchmark");

  // clang-format off
  cli_options.add_options()
    ("s,scale", "Database scale factor (10 ~ 10GB)", cxxopts::value<int32_t>()->default_value("10"));
  // clang-format on

  auto config = std::shared_ptr<BenchmarkConfig>{};
  auto scale_factor = int32_t{};

  // Parse command line args
  const auto cli_parse_result = cli_options.parse(argc, argv);

  if (CLIConfigParser::print_help_if_requested(cli_options, cli_parse_result)) {
    return 0;
  }
  scale_factor = cli_parse_result["scale"].as<int32_t>();

  config = CLIConfigParser::parse_cli_options(cli_parse_result);

  std::cout << "- TPC-DS scale factor is " << scale_factor << '\n';

  const auto query_path = std::string{"resources/benchmark/tpcds/tpcds-result-reproduction/query_qualification"};

  Assert(std::filesystem::is_directory(query_path), "Query path (" + query_path + ") has to be a directory.");
  Assert(std::filesystem::exists(std::filesystem::path{query_path + "/01.sql"}), "Queries have to be available.");

  auto query_generator = std::make_unique<FileBasedBenchmarkItemRunner>(config, query_path, filename_blacklist());
  if (config->verify) {
    // We can only verify the results for scale factor (SF) 1 since the dedicated result sets were obtained on this SF.
    Assert(scale_factor == 1, "TPC-DS result verification can only be performed on scale factor 1 (--scale 1).");
    query_generator->load_dedicated_expected_results(
        std::filesystem::path{"resources/benchmark/tpcds/tpcds-result-reproduction/answer_sets_tbl"});
  }
  auto table_generator = std::make_unique<TPCDSTableGenerator>(scale_factor, config);
  auto benchmark_runner = BenchmarkRunner{*config, std::move(query_generator), std::move(table_generator),
                                          BenchmarkRunner::create_context(*config)};
  if (config->verify) {
    add_indices_to_sqlite("resources/benchmark/tpcds/schema.sql", "resources/benchmark/tpcds/create_indices.sql",
                          benchmark_runner.sqlite_wrapper);
  }
  std::cout << "done.\n";

  benchmark_runner.run();
}
