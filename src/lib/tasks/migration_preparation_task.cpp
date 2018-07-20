#if HYRISE_NUMA_SUPPORT

#include "migration_preparation_task.hpp"

#include <numa.h>
#include <algorithm>
#include <chrono>
#include <ctime>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "chunk_migration_task.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "storage/chunk.hpp"
#include "storage/numa_placement_manager.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/numa_memory_resource.hpp"

namespace opossum {

// Stores information about the current state of the NUMA nodes,
// including an imbalance metric, the temperature metrics for each node,
// and a classification of hot and cold nodes.
struct NodeInfoSet {
  double imbalance;
  std::vector<double> node_temperatures;
  std::vector<NodeID> hot_nodes;
  std::vector<NodeID> cold_nodes;
};

// Contains temperature information about a particular Chunk.
struct ChunkInfo {
  std::string table_name;
  ChunkID id;
  int node;
  double temperature;
  friend bool operator<(const ChunkInfo& l, const ChunkInfo& r) { return l.temperature < r.temperature; }
};

template <class T>
bool contains(const std::vector<T>& vec, T value) {
  return std::find(vec.cbegin(), vec.cend(), value) != vec.cend();
}

double mean(const std::vector<double>& container) {
  if (container.empty()) return {};
  double count = container.size();
  double sum = std::accumulate(container.cbegin(), container.cend(), 0.0);
  return sum / count;
}

// Scales the values of the vector, so that the vector has a sum of 1.0
std::vector<double> scale(const std::vector<double>& container) {
  if (container.empty()) return {};
  double sum = std::accumulate(container.cbegin(), container.cend(), 1.0);
  if (sum == 0) {
    return std::vector<double>(container.size(), 0.0);
  }
  std::vector<double> result(container.size());
  for (size_t i = 0; i < container.size(); i++) {
    result[i] = container[i] / sum;
  }
  return result;
}

// Determines whether a NUMA node has enough capacity by specifying a relative threshold.
bool node_has_capacity(size_t node_id, double threshold = 0.8) {
  size_t total_capacity = numa_node_size(node_id, NULL);
  int64_t dummy;
  size_t free_capacity = numa_node_size(node_id, &dummy);
  return static_cast<double>(total_capacity * (1.0 - threshold)) <= static_cast<double>(free_capacity);
}

double safe_log2(double x) { return x == 0 ? 0 : std::log2(x); }

// The inverted Shannon entropy is used as a metric for imbalance between the NUMA nodes.
double inverted_entropy(const std::vector<double>& node_temperatures) {
  if (node_temperatures.size() == 1) {
    return 0;
  }
  double max_entropy = std::pow(
      -1.0 * node_temperatures.size() * (1.0 / node_temperatures.size() * std::log2(1.0 / node_temperatures.size())),
      4);
  double entropy = std::pow(std::accumulate(node_temperatures.cbegin(), node_temperatures.cend(), 0.0,
                                            [](const double& r, const double& a) { return r - a * safe_log2(a); }),
                            4);
  return (max_entropy - entropy) / max_entropy;
}

// Returns a NodeInfoSet struct that contains the current state of the NUMA nodes
NodeInfoSet compute_node_info(const std::vector<double>& node_temperatures) {
  double avg_temperature = mean(node_temperatures);

  std::vector<NodeID> hot_nodes;
  std::vector<NodeID> cold_nodes;

  for (NodeID i = NodeID(0); static_cast<size_t>(i) < node_temperatures.size(); i++) {
    double node_temperature = node_temperatures[i];
    double temperature = (node_temperature - avg_temperature);
    if (temperature > 0) {
      hot_nodes.push_back(i);
    } else if (node_has_capacity(i)) {
      cold_nodes.push_back(i);
    }
  }

  std::sort(hot_nodes.begin(), hot_nodes.end(), [&node_temperatures](const auto& a, const auto& b) {
    return node_temperatures.at(a) < node_temperatures.at(b);
  });
  std::sort(cold_nodes.begin(), cold_nodes.end(), [&node_temperatures](const auto& a, const auto& b) {
    return node_temperatures.at(a) < node_temperatures.at(b);
  });

  return {/* .imbalance = */ inverted_entropy(node_temperatures),
          /* .node_temperatures = */ node_temperatures,
          /* .hot_nodes = */ hot_nodes,
          /* .cold_nodes = */ cold_nodes};
}

// Computes the temperature of a NUMA node based on the Chunks that reside on that node.
std::vector<double> get_node_temperatures(const std::vector<ChunkInfo>& chunk_infos, size_t node_count) {
  std::vector<double> node_temperatures(node_count);
  for (const auto& chunk_info : chunk_infos) {
    node_temperatures.at(chunk_info.node) += chunk_info.temperature;
  }
  return scale(node_temperatures);
}

// Calculates chunk temperature metrics for all full chunks in all tables
// that are stored in the supplied StorageManager.
std::vector<ChunkInfo> collect_chunk_infos(const StorageManager& storage_manager,
                                           const std::chrono::milliseconds& lookback,
                                           const std::chrono::milliseconds& counter_history_interval) {
  std::vector<ChunkInfo> chunk_infos;
  double sum_temperature = 0.0;
  size_t lookback_samples = lookback.count() / counter_history_interval.count();
  for (const auto& table_name : storage_manager.table_names()) {
    const auto& table = *storage_manager.get_table(table_name);
    const auto chunk_count = table.chunk_count();
    for (ChunkID i = ChunkID(0); i < chunk_count; i++) {
      const auto chunk = table.get_chunk(i);
      if (ChunkMigrationTask::chunk_is_completed(chunk, table.max_chunk_size()) && chunk->has_access_counter()) {
        const double temperature = static_cast<double>(chunk->access_counter()->history_sample(lookback_samples));
        sum_temperature += temperature;
        chunk_infos.emplace_back(ChunkInfo{/* .table_name = */ table_name,
                                           /* .id = */ i,
                                           /* .node = */ MigrationPreparationTask::get_node_id(chunk->get_allocator()),
                                           /* .temperature = */ temperature});
      }
    }
  }
  std::sort(chunk_infos.begin(), chunk_infos.end(), [](const ChunkInfo& a, const ChunkInfo& b) { return b < a; });
  return chunk_infos;
}

MigrationPreparationTask::MigrationPreparationTask(const NUMAPlacementManager::Options& options) : _options(options) {}

// This task first collects temperature metrics of chunks and NUMA nodes,
// identifies chunks that are candidates for migration and schedules migration tasks.
void MigrationPreparationTask::_on_execute() {
  // Collect chunk and NUMA node temperature metrics
  auto chunk_infos =
      collect_chunk_infos(StorageManager::get(), _options.counter_history_range, _options.counter_history_interval);
  size_t chunk_counter = 0;
  NodeInfoSet node_info = compute_node_info(get_node_temperatures(chunk_infos, Topology::get().nodes().size()));

  // Migrations are only considered when the imbalance between the NUMA nodes is high enough.
  if (node_info.imbalance > _options.imbalance_threshold && node_info.cold_nodes.size() > 0) {
    // Identify migration candidates (chunks)
    std::vector<ChunkInfo> migration_candidates;
    for (const auto& chunk_info : chunk_infos) {
      if (chunk_info.node < 0 || contains(node_info.hot_nodes, static_cast<NodeID>(chunk_info.node))) {
        migration_candidates.push_back(chunk_info);
      }
      if (migration_candidates.size() >= _options.migration_count) {
        break;
      }
    }

    // Schedule chunk migration tasks
    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    jobs.reserve(migration_candidates.size());

    for (const auto& migration_chunk : migration_candidates) {
      const auto target_node = node_info.cold_nodes.at(chunk_counter % node_info.cold_nodes.size());
      const auto task = std::make_shared<ChunkMigrationTask>(migration_chunk.table_name,
                                                             std::vector<ChunkID>({migration_chunk.id}), target_node);

      task->schedule(target_node, SchedulePriority::Unstealable);
      jobs.push_back(task);
      chunk_counter++;
    }

    CurrentScheduler::wait_for_tasks(jobs);
  }
}

int MigrationPreparationTask::get_node_id(const PolymorphicAllocator<size_t>& alloc) {
  const auto memory_source = dynamic_cast<NUMAMemoryResource*>(alloc.resource());
  if (memory_source) {
    return memory_source->get_node_id();
  }
  return NUMAMemoryResource::UNDEFINED_NODE_ID;
}

}  // namespace opossum

#else
int migration_preparation_task_dummy;
#endif
