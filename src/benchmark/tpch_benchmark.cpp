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
#include "json.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_queries.hpp"
#include "tpch/tpch_query_generator.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "utils/assert.hpp"
#include "visualization/lqp_visualizer.hpp"
#include "visualization/pqp_visualizer.hpp"

using namespace opossum;  // NOLINT

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
    ("s,scale", "Database scale factor (1.0 ~ 1GB)", cxxopts::value<float>()->default_value("1"))
    ("q,queries", "Specify queries to run (comma-separated query ids, e.g. \"--queries 1,3,19\"), default is all", cxxopts::value<std::string>()) // NOLINT
    ("use_prepared_statements", "Use prepared statements instead of random SQL strings", cxxopts::value<bool>()->default_value("false")); // NOLINT
  // clang-format on

  std::shared_ptr<opossum::BenchmarkConfig> config;
  std::string comma_separated_queries;
  float scale_factor;
  bool use_prepared_statements;

  if (opossum::CLIConfigParser::cli_has_json_config(argc, argv)) {
    // JSON config file was passed in
    const auto json_config = opossum::CLIConfigParser::parse_json_config_file(argv[1]);
    scale_factor = json_config.value("scale", 0.1f);
    comma_separated_queries = json_config.value("queries", std::string(""));

    config = std::make_shared<opossum::BenchmarkConfig>(
        opossum::CLIConfigParser::parse_basic_options_json_config(json_config));

    use_prepared_statements = json_config.value("use_prepared_statements", false);
  } else {
    // Parse regular command line args
    const auto cli_parse_result = cli_options.parse(argc, argv);

    if (CLIConfigParser::print_help_if_requested(cli_options, cli_parse_result)) return 0;

    if (cli_parse_result.count("queries")) {
      comma_separated_queries = cli_parse_result["queries"].as<std::string>();
    }

    scale_factor = cli_parse_result["scale"].as<float>();

    config =
        std::make_shared<opossum::BenchmarkConfig>(opossum::CLIConfigParser::parse_basic_cli_options(cli_parse_result));

    use_prepared_statements = cli_parse_result["use_prepared_statements"].as<bool>();
  }

  std::vector<opossum::QueryID> query_ids;

  // Build list of query ids to be benchmarked and display it
  if (comma_separated_queries.empty()) {
    std::transform(opossum::tpch_queries.begin(), opossum::tpch_queries.end(), std::back_inserter(query_ids),
                   [](auto& pair) { return opossum::QueryID{pair.first - 1}; });
  } else {
    // Split the input into query ids, ignoring leading, trailing, or duplicate commas
    auto query_ids_str = std::vector<std::string>();
    boost::trim_if(comma_separated_queries, boost::is_any_of(","));
    boost::split(query_ids_str, comma_separated_queries, boost::is_any_of(","), boost::token_compress_on);
    std::transform(
        query_ids_str.begin(), query_ids_str.end(), std::back_inserter(query_ids), [](const auto& query_id_str) {
          const auto query_id =
              opossum::QueryID{boost::lexical_cast<opossum::QueryID::base_type, std::string>(query_id_str) - 1};
          DebugAssert(query_id < 22, "There are only 22 TPC-H queries");
          return query_id;
        });
  }

  std::cout << "- Benchmarking Queries: [ ";
  for (const auto& query_id : query_ids) {
    std::cout << (query_id + 1) << ", ";
  }
  std::cout << "]" << std::endl;

  // TODO(leander): Enable support for queries that contain multiple statements requiring execution
  if (config->enable_scheduler) {
    // QueryID{14} represents TPC-H query 15 because we use 0 indexing
    Assert(std::find(query_ids.begin(), query_ids.end(), opossum::QueryID{14}) == query_ids.end(),
           "TPC-H query 15 is not supported for multithreaded benchmarking.");
  }

  auto context = opossum::BenchmarkRunner::create_context(*config);

  Assert(!use_prepared_statements || !config->verify, "SQLite validation does not work with prepared statements");

  if (config->verify) {
    // Hack: We cannot verify TPC-H Q15, thus we remove it from the list of queries
    auto it = std::remove(query_ids.begin(), query_ids.end(), 15 - 1);
    if (it != query_ids.end()) {
      // The problem is that the last part of the query, "DROP VIEW", does not return a table. Since we also have
      // the TPC-H test against a known-to-be-good table, we do not want the additional complexity for handling this
      // in the BenchmarkRunner.
      std::cout << "- Skipping Query 15 because it cannot easily be verified" << std::endl;
      query_ids.erase(it, query_ids.end());
    }
  }

  // Add TPCH-specific information
  context.emplace("scale_factor", scale_factor);
  context.emplace("use_prepared_statements", use_prepared_statements);

  // Run the benchmark
  opossum::BenchmarkRunner(
      *config, std::make_unique<opossum::TPCHQueryGenerator>(use_prepared_statements, scale_factor, query_ids),
      std::make_unique<TpchTableGenerator>(scale_factor, config), context)
      .run();
}
