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
#include "file_based_query_generator.hpp"
#include "file_based_table_generator.hpp"
#include "json.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"
#include "utils/assert.hpp"
#include "utils/sqlite_wrapper.hpp"
#include "utils/timer.hpp"
#include "visualization/lqp_visualizer.hpp"
#include "visualization/pqp_visualizer.hpp"

using namespace opossum;  // NOLINT

int main(int argc, char* argv[]) {
  auto cli_options = opossum::BenchmarkRunner::get_basic_cli_options("TPCDS Benchmark");

  std::shared_ptr<opossum::BenchmarkConfig> config;
  // TODO(MPT) modify scale_factor by cli options
  float scale_factor = 1.0f;

  if (opossum::CLIConfigParser::cli_has_json_config(argc, argv)) {
    // JSON config file was passed in
    const auto json_config = opossum::CLIConfigParser::parse_json_config_file(argv[1]);

    config = std::make_shared<opossum::BenchmarkConfig>(
        opossum::CLIConfigParser::parse_basic_options_json_config(json_config));

  } else {
    // Parse regular command line args
    const auto cli_parse_result = cli_options.parse(argc, argv);

    if (CLIConfigParser::print_help_if_requested(cli_options, cli_parse_result)) {
      return 0;
    }

    config =
        std::make_shared<opossum::BenchmarkConfig>(opossum::CLIConfigParser::parse_basic_cli_options(cli_parse_result));
  }

  // TODO(MPT) investigation: what do we have to do to support multithreaded benchmark?
  Assert(!config->enable_scheduler, "Multithreaded benchmark execution is not supported for TPC-DS");
  // TODO(MPT) investigation: what do we have to do to support SQLite validation?
  // Assert(!config->verify, "SQLite validation does not work for TPCDS benchmark");

  auto context = opossum::BenchmarkRunner::create_context(*config);

  std::cout << "- TPCDS scale factor is " << scale_factor << std::endl;

  // TPCDS FilebasedQueryGenerator specification
  std::optional<std::unordered_set<std::string>> query_subset;
  const auto query_filename_blacklist = std::unordered_set<std::string>{};
  std::string query_path = "resources/benchmark/tpcds/queries";
  std::string table_path = "resources/benchmark/tpcds/tables";

  Assert(std::filesystem::is_directory(query_path), "Query path (" + query_path + ") has to be a directory.");
  Assert(std::filesystem::is_directory(table_path), "Table path (" + table_path + ") has to be a directory.");

  auto query_generator =
      std::make_unique<FileBasedQueryGenerator>(*config, query_path, query_filename_blacklist, query_subset);
  // TODO(MPT) replace this generator by the TPCDSTableGenerator
  auto table_generator = std::make_unique<FileBasedTableGenerator>(config, table_path);
  auto benchmark_runner = BenchmarkRunner{*config, std::move(query_generator), std::move(table_generator), context};

  if (config->verify) {
    // TODO(MPJ) encapsulate this code somewhere else since it us used for the TPCDS and JOIN ORDER BENCHMARK

    // Add indexes to SQLite. This is a hack until we support CREATE INDEX ourselves and pass that on to SQLite.
    // Without this, SQLite would never finish.
    std::cout << "- Adding indexes to SQLite" << std::endl;
    Timer timer;

    // SQLite does not support adding primary keys, so we rename the table, create an empty one from the provided
    // schema and copy the data.
    for (const auto& table_name : StorageManager::get().table_names()) {
      benchmark_runner.sqlite_wrapper->raw_execute_query(std::string{"ALTER TABLE "} + table_name +  // NOLINT
                                                         " RENAME TO " + table_name + "_unindexed");
    }

    // Recreate tables from schema.sql
    std::ifstream schema_file("resources/benchmark/tpcds/schema.sql");
    std::string schema_sql((std::istreambuf_iterator<char>(schema_file)), std::istreambuf_iterator<char>());
    benchmark_runner.sqlite_wrapper->raw_execute_query(schema_sql);

    // Add foreign keys
    std::ifstream foreign_key_file("resources/benchmark/tpcds/create_indices.sql");
    std::string foreign_key_sql((std::istreambuf_iterator<char>(foreign_key_file)), std::istreambuf_iterator<char>());
    benchmark_runner.sqlite_wrapper->raw_execute_query(foreign_key_sql);

    // Copy over data
    for (const auto& table_name : StorageManager::get().table_names()) {
      Timer per_table_time;
      std::cout << "-  Adding indexes to SQLite table " << table_name << std::flush;

      benchmark_runner.sqlite_wrapper->raw_execute_query(std::string{"INSERT INTO "} + table_name +  // NOLINT
                                                         " SELECT * FROM " + table_name + "_unindexed");

      std::cout << " (" << per_table_time.lap_formatted() << ")" << std::endl;
    }

    std::cout << "- Added indexes to SQLite (" << timer.lap_formatted() << ")" << std::endl;
  }

  std::cout << "done." << std::endl;

  benchmark_runner.run();
}
