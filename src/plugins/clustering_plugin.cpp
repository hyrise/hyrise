#include "clustering_plugin.hpp"

#include <iostream>
#include <filesystem>
#include <fstream>
#include <random>

#include "clustering/abstract_clustering_algo.hpp"
#include "clustering/simple_clustering_algo.hpp"
#include "clustering/chunkwise_clustering_algo.hpp"
#include "clustering/disjoint_clusters_algo.hpp"
#include "nlohmann/json.hpp"
#include "resolve_type.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/base_attribute_statistics.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/range_filter.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

std::string ClusteringPlugin::description() const { return "This is the Hyrise ClusteringPlugin"; }

template <typename ColumnDataType>
std::pair<ColumnDataType,ColumnDataType> _get_min_max(const std::shared_ptr<BaseAttributeStatistics>& base_attribute_statistics) {
  const auto attribute_statistics = std::dynamic_pointer_cast<AttributeStatistics<ColumnDataType>>(base_attribute_statistics);
  Assert(attribute_statistics, "could not cast to AttributeStatistics");

  ColumnDataType min;
  ColumnDataType max;

  if constexpr (!std::is_arithmetic_v<ColumnDataType>) {
    Assert(attribute_statistics->min_max_filter, "no min-max filter despite non-arithmetic type");
    min = attribute_statistics->min_max_filter->min;
    max = attribute_statistics->min_max_filter->max;
  } else {
    Assert(attribute_statistics->range_filter, "no range filter despite arithmetic type");
    const auto ranges = attribute_statistics->range_filter->ranges;
    min = ranges[0].first;
    max = ranges.back().second;
  }

  return std::make_pair(min, max);
}

template <typename ColumnDataType>
std::pair<ColumnDataType,ColumnDataType> _get_min_max(const std::shared_ptr<Chunk>& chunk, const ColumnID column_id) {
  const auto pruning_statistics = chunk->pruning_statistics();
  Assert(pruning_statistics, "no pruning statistics");

  return _get_min_max<ColumnDataType>((*pruning_statistics)[column_id]);
}

void _export_chunk_pruning_statistics() {
  const std::string table_name{"lineitem"};
  if (!Hyrise::get().storage_manager.has_table(table_name)) return;
  std::cout << "[ClusteringPlugin] Exporting " <<  table_name << " chunk pruning stats...";

  const auto table = Hyrise::get().storage_manager.get_table(table_name);
  const std::vector<std::string> column_names = {"l_orderkey", "l_shipdate", "l_discount"};

  std::ofstream log(table_name + ".stats");

  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); chunk_id++) {
    const auto& chunk = table->get_chunk(chunk_id);
    if (chunk) {
      for (const auto& column_name : column_names) {
        const auto column_id = table->column_id_by_name(column_name);
        const auto column_data_type = table->column_data_type(column_id);

        resolve_data_type(column_data_type, [&](const auto data_type_t) {
          using ColumnDataType = typename decltype(data_type_t)::type;
          const auto min_max = _get_min_max<ColumnDataType>(chunk, column_id);
          log << min_max.first << "," << min_max.second << "|";
        });
      }
      log << std::endl;
    }
  }
  std::cout << " Done" << std::endl;
}

void _export_chunk_size_statistics() {
  const std::string table_name = "lineitem";
  if (!Hyrise::get().storage_manager.has_table(table_name)) return;
  std::cout << "[ClusteringPlugin] Exporting " <<  table_name << " chunk size stats...";
  const auto& table = Hyrise::get().storage_manager.get_table(table_name);
  std::vector<size_t> chunk_sizes;

  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); chunk_id++) {
    const auto& chunk = table->get_chunk(chunk_id);
    if (chunk) {
      chunk_sizes.push_back(chunk->size());
    }
  }

  std::ofstream log(table_name + ".cs");
  log << "[";
  for (const auto chunk_size : chunk_sizes) {
    log << chunk_size << ", ";
  }
  log << "]" << std::endl;

  std::cout << " Done" << std::endl;
}

std::mutex _cout_mutex;


std::vector<size_t> _executed_updates(3, 0);
std::vector<size_t> _successful_executed_updates(3, 0);
std::mutex _update_mutex;

void _update_rows_multithreaded(const size_t seed) {

  std::vector<size_t> executed_updates(3, 0);
  std::vector<size_t> successful_executed_updates(3, 0);

  while(Hyrise::get().update_thread_state == 0) {
    // wait for the clustering to begin
    std::this_thread::sleep_for (std::chrono::milliseconds(10));
  }

  _cout_mutex.lock();
  std::cout << "Thread " << seed << " started executing updates" << std::endl;
  _cout_mutex.unlock();


  std::mt19937 gen(seed);
  std::uniform_int_distribution<> distrib(1, 60'000'000);
  auto current_step = Hyrise::get().update_thread_state;
  while (current_step < 4) {
    size_t l_orderkey = distrib(gen);
    while (l_orderkey % 32 >= 8) {
      l_orderkey -= 8;
    }
    const auto l_orderkey_as_string = std::to_string(l_orderkey);
    const std::string sql = "UPDATE lineitem SET l_orderkey = "  + l_orderkey_as_string + " WHERE l_orderkey = " + l_orderkey_as_string;

    auto builder = SQLPipelineBuilder{ sql };
    auto sql_pipeline = std::make_unique<SQLPipeline>(builder.create_pipeline());
    const auto [status, table] = sql_pipeline->get_result_tables();
    if (status == SQLPipelineStatus::Success) {

      successful_executed_updates[current_step - 1]++;
    } else {
      _cout_mutex.lock();
      std::cout << "Thread " << seed << ": Update failed at step " << current_step << std::endl;
      _cout_mutex.unlock();
    }
    executed_updates[current_step - 1]++;
    auto new_step = Hyrise::get().update_thread_state;
    if (new_step != current_step) {
      _cout_mutex.lock();
      std::cout << "Thread " << seed << " step changes from " << current_step << " to " << new_step << std::endl;
      std::cout << "Thread " << seed << " executed " << executed_updates[current_step - 1]  << " updates in step " << current_step << std::endl;
      _cout_mutex.unlock();
    }
    current_step = new_step;
  }


  _cout_mutex.lock();
  std::cout << "Thread " << seed << " stopped executing updates" << std::endl;
  std::vector<std::string> step_names = {"partition", "merge", "sort"};
  for (size_t step_index = 0; step_index < step_names.size(); step_index++) {
    const auto total = executed_updates[step_index];
    const auto successful = successful_executed_updates[step_index];
    std::cout << "Thread with seed " << seed << " executed " << total << " updates during the " << step_names[step_index] << " step, " << successful << " (" << 100.0 * successful / total << "%) of them successful." << std::endl;


  }
  std::cout << executed_updates[0] << " " << executed_updates[1] << " " << executed_updates[2] << std::endl;
  std::cout << std::endl;
  _cout_mutex.unlock();

  _update_mutex.lock();
  for (size_t step{0}; step < 3; step++) {
    _executed_updates[step] += executed_updates[step];
    _successful_executed_updates[step] += successful_executed_updates[step];
  }
  _update_mutex.unlock();
}

void ClusteringPlugin::start() {
  _clustering_config = read_clustering_config();

  const auto env_var_algorithm = std::getenv("CLUSTERING_ALGORITHM");
  std::string algorithm;
  if (env_var_algorithm == NULL) {
    algorithm = "DisjointClusters";
  } else {
    algorithm = std::string(env_var_algorithm);
  }

  if (algorithm == "Partitioner") {
    _clustering_algo = std::make_shared<SimpleClusteringAlgo>(_clustering_config);
  } else if (algorithm == "DisjointClusters") {
    _clustering_algo = std::make_shared<DisjointClustersAlgo>(_clustering_config);
  } else {
    Fail("Unknown clustering algorithm: " + algorithm);
  }



  std::cout << "[ClusteringPlugin] Starting clustering, using " << _clustering_algo->description() << std::endl;

  constexpr size_t NUM_UPDATE_THREADS = 10;
  std::vector<std::thread> threads;
  for (size_t thread_index = 0; thread_index < NUM_UPDATE_THREADS; thread_index++) {
    threads.push_back(std::thread(_update_rows_multithreaded, thread_index));
    std::cout << "Started thread " << thread_index << std::endl;
  }
  _clustering_algo->run();
  std::vector<std::string> step_names = {"partition", "merge", "sort"};
  for (size_t step = 0; step < step_names.size(); step++) {
    const auto total = _executed_updates[step];
    const auto successful = _successful_executed_updates[step];
    std::cout << "Executed " << total << " updates during the " << step_names[step] << ", " << successful << " (" << 100.0 * successful / total << "%) of them successful." << std::endl;
  }

  for (size_t thread_index = 0; thread_index < NUM_UPDATE_THREADS; thread_index++) {
    threads[thread_index].join();
    std::cout << "Stopped thread " << thread_index << std::endl;
  }


  _write_clustering_information();

  //_export_chunk_pruning_statistics();
  //_export_chunk_size_statistics();
  std::cout << "[ClusteringPlugin] Clustering complete." << std::endl;
}

void ClusteringPlugin::stop() { }

const ClusteringByTable ClusteringPlugin::read_clustering_config(const std::string& filename) {
  if (!std::filesystem::exists(filename)) {
    std::cout << "clustering config file not found: " << filename << std::endl;
    std::exit(1);
  }

  std::ifstream ifs(filename);
  const auto clustering_config = nlohmann::json::parse(ifs);
  return clustering_config;
}

void ClusteringPlugin::_write_clustering_information() const {
  nlohmann::json clustering_info;
  clustering_info["runtime"] = _clustering_algo->runtime_statistics();
  clustering_info["config"] = _clustering_config;
  clustering_info["algo"] = _clustering_algo->description();

  std::ofstream out_file(".clustering_info.json");
  out_file << clustering_info.dump(2) << std::endl;
  out_file.close();
}

EXPORT_PLUGIN(ClusteringPlugin)

}  // namespace opossum
