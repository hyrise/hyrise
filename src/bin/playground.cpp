#include "hyrise.hpp"
#include "utils/plugin_manager.hpp"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <vector>

#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
#include "cli_config_parser.hpp"
#include "clustering/util.hpp"
#include "cxxopts.hpp"
#include "file_based_benchmark_item_runner.hpp"
#include "file_based_table_generator.hpp"
#include "operators/import.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "tpcds/tpcds_table_generator.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"
#include "tpch/tpch_table_generator.hpp"

#include "SQLParserResult.h"

using namespace opossum;  // NOLINT

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

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cout << "Usage: " + std::string(argv[0]) + " <clustering plugin path> <benchmark option(s)>" << std::endl;
    std::exit(1);
  }

  // determine benchmark to run
  const auto env_var = std::getenv("BENCHMARK_TO_RUN");
  if (env_var == nullptr) {
    std::cerr << "Please pass environment variable \"BENCHMARK_TO_RUN\" to set a target benchmark.\nExiting "
                 "benchmarking-playground."
              << std::endl;
    exit(17);
  }

  const auto BENCHMARKS = std::vector<std::string>{"tpch", "tpcds", "job"};
  auto BENCHMARK = std::string(env_var);
  if (std::find(BENCHMARKS.begin(), BENCHMARKS.end(), BENCHMARK) == BENCHMARKS.end()) {
    std::cerr << "Benchmark \"" << BENCHMARK << "\" not supported. Supported benchmarks: ";
    for (const auto& benchmark : BENCHMARKS) std::cout << "\"" << benchmark << "\" ";
    std::cerr << "\nExiting." << std::flush;
    exit(17);
  }
  std::cout << "Running " << BENCHMARK << " ... " << std::endl;

  // create benchmark config
  auto cli_options = BenchmarkRunner::get_basic_cli_options("Clustering Plugin Benchmark Runner");

  // clang-format off
  cli_options.add_options()
    ("s,scale", "Database scale factor (1.0 ~ 1GB)", cxxopts::value<float>()->default_value("1"))
    ("cluster_only", "Do not benchmark, exit after the clustering", cxxopts::value<bool>()->default_value("false"));

  // clang-format on

  const auto cli_parse_result = cli_options.parse(argc, argv);

  auto config = std::make_shared<BenchmarkConfig>(CLIConfigParser::parse_cli_options(cli_parse_result));
  config->cache_binary_tables = false;
  config->metrics = true;
  Assert(config->output_file_path, "you must provide an output file");
  std::string output_file_path = *config->output_file_path;
  const auto cluster_only = cli_parse_result["cluster_only"].as<bool>();

  // init benchmark runner
  bool plugin_loaded = false;
  std::vector<std::string> result_file_names;
  if (BENCHMARK == "tpch") {
    const auto scale_factor = cli_parse_result["scale"].as<float>();
    std::cout << "- Scale factor is " << scale_factor << std::endl;
    for (auto query_id = 0u; query_id < 22; query_id++) {
      //for (auto query_id = 5u; query_id < 6; query_id++) {
      if (config->verify && query_id == 14) continue;

      if (plugin_loaded) Assert(Hyrise::get().storage_manager.has_table("lineitem"), "lineitem disappeared");
      const std::vector<BenchmarkItemID> tpch_query_ids_benchmark = {BenchmarkItemID{query_id}};
      std::stringstream query_name_stream;
      query_name_stream << std::setw(2) << std::setfill('0') << (query_id + 1);
      config->output_file_path = output_file_path + "." + query_name_stream.str();
      result_file_names.push_back(*config->output_file_path);

      auto item_runner =
          std::make_unique<TPCHBenchmarkItemRunner>(config, false, scale_factor, tpch_query_ids_benchmark);
      auto benchmark_runner = std::make_shared<BenchmarkRunner>(
          *config, std::move(item_runner), std::make_unique<TPCHTableGenerator>(scale_factor, config),
          BenchmarkRunner::create_context(*config));
      Hyrise::get().benchmark_runner = benchmark_runner;

      if (!plugin_loaded) {
        //const std::string sql = "DELETE FROM lineitem WHERE l_orderkey = 1";
        //std::cout << "Executing SQL: " << sql << std::endl;
        //auto sql_pipeline = std::make_unique<SQLPipeline>(SQLPipelineBuilder(sql).create_pipeline());
        //sql_pipeline->get_result_table();

        const std::string plugin_filename = argv[1];
        const std::filesystem::path plugin_path(plugin_filename);
        Hyrise::get().plugin_manager.load_plugin(plugin_path);
        plugin_loaded = true;

        if (cluster_only) {
          return 0;
        }
      }

      // actually run the benchmark
      benchmark_runner->run();

      // after the benchmark was executed, add more interesting statistics to the json.
      // we could also modify the benchmark to directly export this information, but that feels hacky.
      if (!(config->enable_visualization || config->verify)) _append_additional_statistics(*config->output_file_path);
    }

    if (!(config->enable_visualization || config->verify)) _merge_result_files(output_file_path, result_file_names);

    // run each query twice again and visualize it
    if (!config->enable_visualization) {
      auto tpch_query_ids_benchmark = std::vector<BenchmarkItemID>();
      for (size_t i = 0u; i < 22; i++) {
        tpch_query_ids_benchmark.push_back(BenchmarkItemID{i});
      }
      auto item_runner =
          std::make_unique<TPCHBenchmarkItemRunner>(config, false, scale_factor, tpch_query_ids_benchmark);
      config->max_runs = 2;
      config->enable_visualization = true;
      auto benchmark_runner = std::make_shared<BenchmarkRunner>(
          *config, std::move(item_runner), std::make_unique<TPCHTableGenerator>(scale_factor, config),
          BenchmarkRunner::create_context(*config));
      Hyrise::get().benchmark_runner = benchmark_runner;
      benchmark_runner->run();
    }
  } else if (BENCHMARK == "tpcds") {
    const std::string query_path = "resources/benchmark/tpcds/tpcds-result-reproduction/query_qualification/";
    const auto scale_factor = cli_parse_result["scale"].as<float>();
    std::cout << "- Scale factor is " << scale_factor << std::endl;
    auto query_files = tpcds_filename_whitelist();
    std::vector<std::string> result_file_names{};

    for (const auto& query_file : query_files) {
      config->output_file_path = output_file_path + "." + query_file;
      result_file_names.push_back(*config->output_file_path);

      auto query_generator = std::make_unique<FileBasedBenchmarkItemRunner>(config, query_path + query_file,
                                                                            std::unordered_set<std::string>{});
      auto table_generator = std::make_unique<TPCDSTableGenerator>(scale_factor, config);
      auto benchmark_runner =
          std::make_shared<BenchmarkRunner>(*config, std::move(query_generator), std::move(table_generator),
                                            opossum::BenchmarkRunner::create_context(*config));
      Hyrise::get().benchmark_runner = benchmark_runner;

      if (!plugin_loaded) {
        // load shuffled tables
        std::vector<std::string> shuffled_tables{"store_sales"};
        for (const auto& table_name : shuffled_tables) {
          auto importer = std::make_shared<Import>("shuffled_" + table_name + ".csv", table_name);
          std::cout << "Replacing " << table_name << " with a shuffled version.." << std::flush;
          importer->execute();
          auto table = Hyrise::get().storage_manager.get_table(table_name);
          for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); chunk_id++) {
            const auto& chunk = table->get_chunk(chunk_id);
            if (chunk && chunk->is_mutable()) {
              chunk->finalize();
            }
          }
          //ChunkEncoder::encode_all_chunks(Hyrise::get().storage_manager.get_table(table_name), EncodingType::Dictionary);
          std::cout << " done" << std::endl;
        }

        const std::string plugin_filename = argv[1];
        const std::filesystem::path plugin_path(plugin_filename);
        Hyrise::get().plugin_manager.load_plugin(plugin_path);
        plugin_loaded = true;

        if (cluster_only) {
          return 0;
        }
      }

      // actually run the benchmark
      benchmark_runner->run();

      // after the benchmark was executed, add more interesting statistics to the json.
      // we could also modify the benchmark to directly export this information, but that feels hacky.
      if (!(config->enable_visualization || config->verify)) _append_additional_statistics(*config->output_file_path);
    }
    if (!(config->enable_visualization || config->verify)) _merge_result_files(output_file_path, result_file_names);

    // run each query twice again and visualize it
    if (!config->enable_visualization) {
      auto tpch_query_ids_benchmark = std::vector<BenchmarkItemID>();
      for (size_t i = 0u; i < 22; i++) {
        tpch_query_ids_benchmark.push_back(BenchmarkItemID{i});
      }
      config->max_runs = 2;
      config->enable_visualization = true;
      auto query_generator = std::make_unique<FileBasedBenchmarkItemRunner>(config, query_path, filename_blacklist());
      auto table_generator = std::make_unique<TPCDSTableGenerator>(scale_factor, config);
      auto benchmark_runner =
          std::make_shared<BenchmarkRunner>(*config, std::move(query_generator), std::move(table_generator),
                                            opossum::BenchmarkRunner::create_context(*config));
      Hyrise::get().benchmark_runner = benchmark_runner;
      benchmark_runner->run();
    }
  } else if (BENCHMARK == "job") {
    const auto table_path = "hyrise/imdb_data";
    const auto query_path = "hyrise/third_party/join-order-benchmark";
    const auto non_query_file_names = std::unordered_set<std::string>{"fkindexes.sql", "schema.sql"};

    auto benchmark_item_runner =
        std::make_unique<FileBasedBenchmarkItemRunner>(config, query_path, non_query_file_names);
    auto table_generator = std::make_unique<FileBasedTableGenerator>(config, table_path);
    auto benchmark_runner =
        std::make_shared<BenchmarkRunner>(*config, std::move(benchmark_item_runner), std::move(table_generator),
                                          BenchmarkRunner::create_context(*config));

    Hyrise::get().benchmark_runner = benchmark_runner;

    // actually run the benchmark
    benchmark_runner->run();

    _append_additional_statistics(*config->output_file_path);
  }

  return 0;
}
