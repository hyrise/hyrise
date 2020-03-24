#include "hyrise.hpp"
#include "utils/plugin_manager.hpp"


#include <iostream>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <sstream>
#include <vector>

#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
#include "cli_config_parser.hpp"
#include "cxxopts.hpp"
#include "file_based_benchmark_item_runner.hpp"
#include "file_based_table_generator.hpp"
#include "operators/get_table.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "tpcds/tpcds_table_generator.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"
#include "tpch/tpch_table_generator.hpp"


#include "SQLParserResult.h"

using namespace opossum;  // NOLINT

const nlohmann::json _read_clustering_config(const std::string& filename) {
  if (!std::filesystem::exists(filename)) {
    std::cout << "clustering config file not found: " << filename << std::endl;
    std::exit(1);
  }

  std::ifstream ifs(filename);
  const auto clustering_config = nlohmann::json::parse(ifs);
  return clustering_config;
}

void _extract_get_tables(const std::shared_ptr<const AbstractOperator> pqp_node, std::set<std::shared_ptr<const GetTable>>& get_table_operators) {
  if (pqp_node->type() == OperatorType::GetTable) {
    auto get_table_op = std::dynamic_pointer_cast<const GetTable>(pqp_node);
    Assert(get_table_op, "could not cast to GetTable");
    get_table_operators.insert(get_table_op);
  } else {
    if (pqp_node->input_left()) _extract_get_tables(pqp_node->input_left(), get_table_operators);
    if (pqp_node->input_right()) _extract_get_tables(pqp_node->input_right(), get_table_operators);
  }
}

const nlohmann::json _compute_pruned_chunks_per_table() {
  std::map<std::string, std::vector<size_t>> pruned_chunks_per_table;

  for (auto iter = Hyrise::get().default_pqp_cache->unsafe_begin(); iter != Hyrise::get().default_pqp_cache->unsafe_end(); ++iter) {
    const auto& [query_string, physical_query_plan] = *iter;

    std::set<std::shared_ptr<const GetTable>> get_table_operators;
    _extract_get_tables(physical_query_plan, get_table_operators);

    // Queries are cached just once (per parameter combination).
    // Thus, we need to check how often the concrete queries were executed.
    auto& gdfs_cache = dynamic_cast<GDFSCache<std::string, std::shared_ptr<AbstractOperator>>&>(Hyrise::get().default_pqp_cache->unsafe_cache());
    const size_t frequency = gdfs_cache.frequency(query_string);
    Assert(frequency > 0, "found a pqp for a query that was not cached");

    for (const auto& get_table : get_table_operators) {
      const auto& table_name = get_table->table_name();
      const auto& number_of_pruned_chunks = get_table->pruned_chunk_ids().size();
      for (size_t run{0}; run < frequency; run++) {
        pruned_chunks_per_table[table_name].push_back(number_of_pruned_chunks);
      }
    }
  }

  return pruned_chunks_per_table;
}

void _append_additional_statistics(const std::string& result_file_path) {
      std::ifstream benchmark_result_file(result_file_path);
      auto benchmark_result_json = nlohmann::json::parse(benchmark_result_file);

      const auto benchmark_count = benchmark_result_json["benchmarks"].size();
      Assert(benchmark_count == 1, "expected " + result_file_path + " file containing exactly one benchmark, but it contains " + std::to_string(benchmark_count));
      const std::string query_name = benchmark_result_json["benchmarks"].at(0)["name"];

      // store clustering config and chunk pruning stats
      const auto clustering_config_json = _read_clustering_config("clustering_config.json");
      benchmark_result_json["clustering_config"] = clustering_config_json;

      benchmark_result_json["pruning_stats"][query_name] = _compute_pruned_chunks_per_table();

      // write results back
      std::ofstream final_result_file(result_file_path);
      final_result_file << benchmark_result_json.dump(2) << std::endl;
      final_result_file.close();
}

void _merge_result_files(const std::string& merge_result_file_name, const std::vector<std::string>& merge_input_file_names) {
  Assert(!merge_input_file_names.empty(), "you have to provide file names to merge");
  nlohmann::json merge_result_json;

  for (const auto& file_name : merge_input_file_names) {
    std::ifstream benchmark_result_file(file_name);
    auto benchmark_result_json = nlohmann::json::parse(benchmark_result_file);
    const auto benchmark_count = benchmark_result_json["benchmarks"].size();
    Assert(benchmark_count == 1, "expected " + file_name + " file containing exactly one benchmark, but it contains " + std::to_string(benchmark_count));
    const auto pruning_stats_count = benchmark_result_json["pruning_stats"].size();
    Assert(pruning_stats_count == 1, "expected " + file_name + " file containing exactly pruning stats for just one query, but it contains " + std::to_string(pruning_stats_count));

    if (merge_result_json.empty()) {
      std::cout << "merge result is empty" << std::endl;
      merge_result_json = benchmark_result_json;
    } else {
      const auto benchmark = benchmark_result_json["benchmarks"].at(0);
      const std::string query_name = benchmark["name"];
      merge_result_json["benchmarks"].push_back(benchmark);
      merge_result_json["pruning_stats"][query_name] = benchmark_result_json["pruning_stats"][query_name];
    }
  }
  // write results back
  std::ofstream final_result_file(merge_result_file_name);
  final_result_file << merge_result_json.dump(2) << std::endl;
  final_result_file.close();
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cout <<  "Usage: " + std::string(argv[0]) + " <clustering plugin path> <benchmark option(s)>" << std::endl;
    std::exit(1);
  }

  // determine benchmark to run
  const auto env_var = std::getenv("BENCHMARK_TO_RUN");
  if (env_var == nullptr) {
    std::cerr << "Please pass environment variable \"BENCHMARK_TO_RUN\" to set a target benchmark.\nExiting benchmarking-playground." << std::endl;
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
  auto cli_options = BenchmarkRunner::get_basic_cli_options("TPC-H Benchmark");

  // clang-format off
  cli_options.add_options()
    ("s,scale", "Database scale factor (1.0 ~ 1GB)", cxxopts::value<float>()->default_value("1"));
  // clang-format on

  const auto cli_parse_result = cli_options.parse(argc, argv);

  auto config = std::make_shared<BenchmarkConfig>(CLIConfigParser::parse_basic_cli_options(cli_parse_result));
  config->max_runs = -1;
  config->cache_binary_tables = false;
  config->sql_metrics = true;
  config->enable_visualization = false;
  Assert(config->output_file_path, "you must provide an output file");
  std::string output_file_path = *config->output_file_path;

  // init benchmark runner
  bool plugin_loaded = false;
  std::vector<std::string> result_file_names;
  if (BENCHMARK == "tpch") {
    const auto scale_factor = cli_parse_result["scale"].as<float>();
    std::cout << "- Scale factor is " << scale_factor << std::endl;
    for (auto query_id = 0u; query_id < 22; query_id++) {
      if (plugin_loaded) Assert(Hyrise::get().storage_manager.has_table("lineitem"), "lineitem disappeared");
      const std::vector<BenchmarkItemID> tpch_query_ids_benchmark = {BenchmarkItemID{query_id}};

      std::stringstream query_name_stream;
      query_name_stream << std::setw(2) << std::setfill('0') << (query_id + 1);
      config->output_file_path = output_file_path + "." + query_name_stream.str();
      result_file_names.push_back(*config->output_file_path);

      auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(config, false, scale_factor, tpch_query_ids_benchmark);
      auto benchmark_runner = std::make_shared<BenchmarkRunner>(
          *config, std::move(item_runner), std::make_unique<TPCHTableGenerator>(scale_factor, config), BenchmarkRunner::create_context(*config));
      Hyrise::get().benchmark_runner = benchmark_runner;

      Assert(!Hyrise::get().storage_manager.get_table("orders")->get_soft_unique_constraints().empty(), "unique constraint lost");

      if (!plugin_loaded) {
        const std::string plugin_filename = argv[1];
        const std::filesystem::path plugin_path(plugin_filename);
        Hyrise::get().plugin_manager.load_plugin(plugin_path);
        plugin_loaded = true;
      }

      // actually run the benchmark
      benchmark_runner->run();



      // after the benchmark was executed, add more interesting statistics to the json.
      // we could also modify the benchmark to directly export this information, but that feels hacky.
      _append_additional_statistics(*config->output_file_path);
    }

    _merge_result_files(output_file_path, result_file_names);
  } else if (BENCHMARK == "tpcds") {
    const std::string query_path = "hyrise/resources/benchmark/tpcds/tpcds-result-reproduction/query_qualification";
    const auto scale_factor = cli_parse_result["scale"].as<float>();
    std::cout << "- scale factor is " << scale_factor << std::endl;

    auto query_generator = std::make_unique<FileBasedBenchmarkItemRunner>(config, query_path, std::unordered_set<std::string>{});
    auto table_generator = std::make_unique<TpcdsTableGenerator>(scale_factor, config);
    auto benchmark_runner = std::make_shared<BenchmarkRunner>(*config, std::move(query_generator), std::move(table_generator),
                                                              opossum::BenchmarkRunner::create_context(*config));
    Hyrise::get().benchmark_runner = benchmark_runner;

    // actually run the benchmark
    benchmark_runner->run();

    _append_additional_statistics(*config->output_file_path);
  } else if (BENCHMARK == "job") {
    const auto table_path = "hyrise/imdb_data";
    const auto query_path = "hyrise/third_party/join-order-benchmark";
    const auto non_query_file_names = std::unordered_set<std::string>{"fkindexes.sql", "schema.sql"};

    auto benchmark_item_runner = std::make_unique<FileBasedBenchmarkItemRunner>(config, query_path, non_query_file_names);
    auto table_generator = std::make_unique<FileBasedTableGenerator>(config, table_path);
    auto benchmark_runner = std::make_shared<BenchmarkRunner>(*config, std::move(benchmark_item_runner), std::move(table_generator),
                                                              BenchmarkRunner::create_context(*config));

    Hyrise::get().benchmark_runner = benchmark_runner;

      // actually run the benchmark
      benchmark_runner->run();

      _append_additional_statistics(*config->output_file_path);
  }


  return 0;
}
