#include "hyrise.hpp"
#include "utils/plugin_manager.hpp"

#include <iostream>
#include <filesystem>
#include <fstream>
#include <map>
#include <vector>

#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
#include "file_based_benchmark_item_runner.hpp"
#include "file_based_table_generator.hpp"
#include "operators/get_table.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "tpcds/tpcds_table_generator.hpp"

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

int main(int argc, const char* argv[]) {
  if (argc != 3) {
    std::cout <<  "Usage: " + std::string(argv[0]) + " <clustering plugin path> <benchmark output file path>" << std::endl;
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
  auto config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
  config->max_duration = std::chrono::seconds(60);
  config->chunk_size = 25'000;
  config->output_file_path = argv[2];

  config->max_runs = -1;
  config->cache_binary_tables = false;
  config->sql_metrics = true;
  config->enable_visualization = false;


  constexpr auto USE_PREPARED_STATEMENTS = false;
  auto SCALE_FACTOR = 17.0f;  // later overwritten


  std::shared_ptr<BenchmarkRunner> benchmark_runner;

  //TODO: parameter scale, chunksize und runtime steuerbar machen

  // init benchmark runner
  if (BENCHMARK == "tpch") {
    SCALE_FACTOR = 1.0f;
    config->max_duration = std::chrono::seconds(60);
    // const std::vector<BenchmarkItemID> tpch_query_ids_benchmark = {BenchmarkItemID{5}};
    // auto item_runner = std::make_ unique<TPCHBenchmarkItemRunner>(config, USE_PREPARED_STATEMENTS, SCALE_FACTOR, tpch_query_ids_benchmark);
    auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(config, USE_PREPARED_STATEMENTS, SCALE_FACTOR);
    benchmark_runner = std::make_shared<BenchmarkRunner>(
        *config, std::move(item_runner), std::make_unique<TPCHTableGenerator>(SCALE_FACTOR, config), BenchmarkRunner::create_context(*config));
    Hyrise::get().benchmark_runner = benchmark_runner;
  } else if (BENCHMARK == "tpcds") {
    SCALE_FACTOR = 1.0f;
    const std::string query_path = "hyrise/resources/benchmark/tpcds/tpcds-result-reproduction/query_qualification";

    auto query_generator = std::make_unique<FileBasedBenchmarkItemRunner>(config, query_path, std::unordered_set<std::string>{});
    auto table_generator = std::make_unique<TpcdsTableGenerator>(SCALE_FACTOR, config);
    benchmark_runner = std::make_shared<BenchmarkRunner>(*config, std::move(query_generator), std::move(table_generator),
                                                              opossum::BenchmarkRunner::create_context(*config));
    Hyrise::get().benchmark_runner = benchmark_runner;
  } else if (BENCHMARK == "job") {
    const auto table_path = "hyrise/imdb_data";
    const auto query_path = "hyrise/third_party/join-order-benchmark";
    const auto non_query_file_names = std::unordered_set<std::string>{"fkindexes.sql", "schema.sql"};

    auto benchmark_item_runner = std::make_unique<FileBasedBenchmarkItemRunner>(config, query_path, non_query_file_names);
    auto table_generator = std::make_unique<FileBasedTableGenerator>(config, table_path);
    benchmark_runner = std::make_shared<BenchmarkRunner>(*config, std::move(benchmark_item_runner), std::move(table_generator),
                                                              BenchmarkRunner::create_context(*config));

    Hyrise::get().benchmark_runner = benchmark_runner;
  }

  // load plugins, e.g. the clustering-Plugin
  const std::string plugin_filename = argv[1];
  const std::filesystem::path plugin_path(plugin_filename);
  //const auto plugin_name = plugin_name_from_path(plugin_path);
  Hyrise::get().plugin_manager.load_plugin(plugin_path);


  // actually run the benchmark
  benchmark_runner->run();

  // after the benchmark was executed, add more interesting statistics to the json.
  // we could also modify the benchmark to directly export this information, but that feels hacky.
  Assert(config->output_file_path, "you must specify an output file path");
  std::ifstream benchmark_result_file(*config->output_file_path);
  auto benchmark_result_json = nlohmann::json::parse(benchmark_result_file);
  benchmark_result_file.close();

  const auto clustering_config_json = _read_clustering_config("clustering_config.json");

  // store clustering config
  benchmark_result_json["clustering_config"] = clustering_config_json;
  benchmark_result_json["number_of_pruned_chunks"] = _compute_pruned_chunks_per_table();


  std::ofstream final_result_file(*config->output_file_path);
  final_result_file << benchmark_result_json.dump(2) << std::endl;
  final_result_file.close();

  return 0;
}
