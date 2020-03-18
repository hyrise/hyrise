#include "hyrise.hpp"
#include "utils/plugin_manager.hpp"


#include <iostream>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <vector>

#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
#include "operators/get_table.hpp"
#include "sql/sql_pipeline_builder.hpp"


#include "SQLParserResult.h"

#include "clustering/benchmark_parsers.hpp"


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

  // init benchmark runner
  std::shared_ptr<BenchmarkRunner> benchmark_runner;

  if (BENCHMARK == "tpch") {
    benchmark_runner = parse_tpch_args(argc, argv);
  } else if (BENCHMARK == "tpcds") {
    benchmark_runner = parse_tpcds_args(argc, argv);
  } else if (BENCHMARK == "job") {
    benchmark_runner = parse_job_args(argc, argv);
  }
  Hyrise::get().benchmark_runner = benchmark_runner;

  // load plugins, e.g. the clustering-Plugin
  const std::string plugin_filename = argv[1];
  const std::filesystem::path plugin_path(plugin_filename);
  Hyrise::get().plugin_manager.load_plugin(plugin_path);


  // actually run the benchmark
  benchmark_runner->run();

  // after the benchmark was executed, add more interesting statistics to the json.
  // we could also modify the benchmark to directly export this information, but that feels hacky.
  Assert(benchmark_runner->_config.output_file_path, "you must specify an output file path");
  const std::string& output_file_path = *benchmark_runner->_config.output_file_path;

  std::ifstream benchmark_result_file(output_file_path);
  auto benchmark_result_json = nlohmann::json::parse(benchmark_result_file);
  benchmark_result_file.close();

  // store clustering config and chunk pruning stats
  const auto clustering_config_json = _read_clustering_config("clustering_config.json");
  benchmark_result_json["clustering_config"] = clustering_config_json;

  benchmark_result_json["number_of_pruned_chunks"] = _compute_pruned_chunks_per_table();

  // write results back
  std::ofstream final_result_file(output_file_path);
  final_result_file << benchmark_result_json.dump(2) << std::endl;
  final_result_file.close();

  return 0;
}
