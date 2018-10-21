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
#include "cxxopts.hpp"
#include "json.hpp"
#include "planviz/lqp_visualizer.hpp"
#include "planviz/sql_query_plan_visualizer.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "tpch/tpch_queries.hpp"

/**
 * This benchmark measures Hyrise's performance executing the TPC-H *queries*, it doesn't (yet) support running the
 * TPC-H *benchmark* exactly as it is specified.
 * (Among other things, the TPC-H requires performing data refreshes and has strict requirements for the number of
 * sessions running in parallel. See http://www.tpc.org/tpch/default.asp for more info)
 * The benchmark offers a wide range of options (scale_factor, chunk_size, ...) but most notably it offers two modes:
 * IndividualQueries and PermutedQuerySets. See docs on BenchmarkMode for details.
 * The benchmark will stop issuing new queries if either enough iterations have taken place or enough time has passed.
 *
 * main() is mostly concerned with parsing the CLI options while BenchmarkRunner.run() performs the actual benchmark
 * logic.
 */

int main(int argc, char* argv[]) {
  auto cli_options = opossum::BenchmarkRunner::get_basic_cli_options("TPCH Benchmark");

  // clang-format off
  cli_options.add_options()
    ("s,scale", "Database scale factor (1.0 ~ 1GB)", cxxopts::value<float>()->default_value("0.1"))
    ("q,queries", "Specify queries to run (comma-separated query ids, e.g. \"--queries 1,3,19\"), default is all", cxxopts::value<std::string>()); // NOLINT
  // clang-format on

  std::unique_ptr<opossum::BenchmarkConfig> config;
  std::string comma_separated_queries;
  float scale_factor;

  if (opossum::CLIConfigParser::cli_has_json_config(argc, argv)) {
    // JSON config file was passed in
    const auto json_config = opossum::CLIConfigParser::parse_json_config_file(argv[1]);
    scale_factor = json_config.value("scale", 0.1f);
    comma_separated_queries = json_config.value("queries", std::string(""));

    config = std::make_unique<opossum::BenchmarkConfig>(
        opossum::CLIConfigParser::parse_basic_options_json_config(json_config));

  } else {
    // Parse regular command line args
    const auto cli_parse_result = cli_options.parse(argc, argv);

    // Display usage and quit
    if (cli_parse_result.count("help")) {
      std::cout << opossum::CLIConfigParser::detailed_help(cli_options) << std::endl;
      return 0;
    }
    if (cli_parse_result.count("queries")) {
      comma_separated_queries = cli_parse_result["queries"].as<std::string>();
    }

    scale_factor = cli_parse_result["scale"].as<float>();

    config =
        std::make_unique<opossum::BenchmarkConfig>(opossum::CLIConfigParser::parse_basic_cli_options(cli_parse_result));
  }

  std::vector<opossum::QueryID> query_ids;

  // Build list of query ids to be benchmarked and display it
  if (comma_separated_queries.empty()) {
    std::transform(opossum::tpch_queries.begin(), opossum::tpch_queries.end(), std::back_inserter(query_ids),
                   [](auto& pair) { return pair.first; });
  } else {
    // Split the input into query ids, ignoring leading, trailing, or duplicate commas
    auto query_ids_str = std::vector<std::string>();
    boost::trim_if(comma_separated_queries, boost::is_any_of(","));
    boost::split(query_ids_str, comma_separated_queries, boost::is_any_of(","), boost::token_compress_on);
    std::transform(query_ids_str.begin(), query_ids_str.end(), std::back_inserter(query_ids),
                   boost::lexical_cast<opossum::QueryID, std::string>);
  }

  config->out << "- Benchmarking Queries: [ ";
  for (const auto query_id : query_ids) {
    config->out << (query_id) << ", ";
  }
  config->out << "]" << std::endl;

  // TODO(leander): Enable support for queries that contain multiple statements requiring execution
  if (config->enable_scheduler) {
    Assert(std::find(query_ids.begin(), query_ids.end(), opossum::QueryID{15}) == query_ids.end(),
           "TPC-H query 15 is not supported for multithreaded benchmarking.");
  }

  // Set up TPCH benchmark
  opossum::NamedQueries queries;
  queries.reserve(query_ids.size());

  for (const auto query_id : query_ids) {
    queries.emplace_back("TPC-H " + std::to_string(query_id), opossum::tpch_queries.at(query_id));
  }

  config->out << "- Generating TPCH Tables with scale_factor=" << scale_factor << " ..." << std::endl;

  const auto tables = opossum::TpchDbGenerator(scale_factor, config->chunk_size).generate();

  for (auto& tpch_table : tables) {
    const auto& table_name = opossum::tpch_table_names.at(tpch_table.first);
    auto& table = tpch_table.second;

    opossum::BenchmarkTableEncoder::encode(table_name, table, config->encoding_config);
    opossum::StorageManager::get().add_table(table_name, table);
  }
  config->out << "- ... done." << std::endl;

  auto context = opossum::BenchmarkRunner::create_context(*config);

  // Add TPCH-specific information
  context.emplace("scale_factor", scale_factor);

  // Run the benchmark
  opossum::BenchmarkRunner(*config, queries, context).run();
}
