#include "clustering_plugin.hpp"

#include <iostream>
#include <filesystem>
#include <fstream>

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

namespace opossum {

const std::string ClusteringPlugin::description() const { return "This is the Hyrise ClusteringPlugin"; }

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

void ClusteringPlugin::start() {
  const auto clustering_config = _read_clustering_config();
  //std::shared_ptr<AbstractClusteringAlgo> clustering_algo = std::make_shared<SimpleClusteringAlgo>(SimpleClusteringAlgo(clustering_config));
  std::shared_ptr<AbstractClusteringAlgo> clustering_algo = std::make_shared<DisjointClustersAlgo>(DisjointClustersAlgo(clustering_config));

  std::cout << "[ClusteringPlugin] Starting clustering, using " << clustering_algo->description() << std::endl;

  clustering_algo->run();

  std::cout << "[ClusteringPlugin] Clustering complete." << std::endl;

  _export_chunk_pruning_statistics();
  _export_chunk_size_statistics();
}

void ClusteringPlugin::stop() { }

const ClusteringByTable ClusteringPlugin::_read_clustering_config(const std::string& filename) const {
  if (!std::filesystem::exists(filename)) {
    std::cout << "clustering config file not found: " << filename << std::endl;
    std::exit(1);
  }

  std::ifstream ifs(filename);
  const auto clustering_config = nlohmann::json::parse(ifs);
  return clustering_config;
}

EXPORT_PLUGIN(ClusteringPlugin)

}  // namespace opossum
