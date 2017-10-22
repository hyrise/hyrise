#include "migration_preparation_task.hpp"

#if HYRISE_NUMA_SUPPORT

#include <numa.h>
#include <algorithm>
#include <chrono>
#include <ctime>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "chunk_migration_task.hpp"
#include "polymorphic_allocator.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/chunk.hpp"
#include "storage/numa_placement_manager.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/numa_memory_resource.hpp"

namespace opossum {

// TODO(normanrz): Remove
template <class T>
void print_vector(const std::vector<T>& vec, std::string prefix = "", std::string sep = " ") {
  std::cout << prefix;
  for (const auto& a : vec) {
    std::cout << sep << a;
  }
  std::cout << std::endl;
}

// TODO(normanrz): Comment
// TODO(normanrz): Rename `hot_nodes` to `node_infos` or something
struct NodeInfoSet {
  double imbalance;
  std::vector<double> node_temperature;
  std::vector<NodeID> hot_nodes;
  std::vector<NodeID> cold_nodes;
};

// TODO(normanrz): Comment
struct ChunkInfo {
  std::string table_name;
  ChunkID id;
  int node;
  size_t byte_size;
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

bool node_has_capacity(size_t node_id, double threshold = 0.8) {
  size_t total_capacity = numa_node_size(node_id, NULL);
  int64_t dummy;
  size_t free_capacity = numa_node_size(node_id, &dummy);
  return static_cast<double>(total_capacity * (1.0 - threshold)) <= static_cast<double>(free_capacity);
}

double safe_log2(double x) { return x == 0 ? 0 : std::log2(x); }

double inverted_entropy(const std::vector<double>& node_chunk) {
  double max_entropy =
      std::pow(-1.0 * node_chunk.size() * (1.0 / node_chunk.size() * std::log2(1.0 / node_chunk.size())), 4);
  double entropy = std::pow(std::accumulate(node_chunk.cbegin(), node_chunk.cend(), 0.0,
                                            [](const double& r, const double& a) { return r - a * safe_log2(a); }),
                            4);
  return (max_entropy - entropy) / max_entropy;
}

// TODO(normanrz): Comment
NodeInfoSet find_hot_nodes(const std::vector<double>& node_chunk) {
  double avg_tasks = mean(node_chunk);

  std::vector<double> node_temperature(node_chunk.size());
  std::vector<NodeID> hot_nodes;
  std::vector<NodeID> cold_nodes;

  for (NodeID i = NodeID(0); static_cast<size_t>(i) < node_chunk.size(); i++) {
    double a = node_chunk[i];
    double temperature = (a - avg_tasks);
    node_temperature[i] = a;
    if (temperature > 0) {
      hot_nodes.push_back(i);
    } else if (node_has_capacity(i)) {
      cold_nodes.push_back(i);
    }
  }

  std::sort(hot_nodes.begin(), hot_nodes.end(), [&node_temperature](const auto& a, const auto& b) {
    return node_temperature.at(a) < node_temperature.at(b);
  });
  std::sort(cold_nodes.begin(), cold_nodes.end(), [&node_temperature](const auto& a, const auto& b) {
    return node_temperature.at(a) < node_temperature.at(b);
  });

  return {/* .imbalance = */ inverted_entropy(node_chunk),
          /* .node_temperature = */ node_temperature,
          /* .hot_nodes = */ hot_nodes,
          /* .cold_nodes = */ cold_nodes};
}

// TODO(normanrz): Comment
std::vector<double> get_node_temperature(const std::vector<ChunkInfo>& chunk_infos, size_t node_count) {
  std::vector<double> node_temperature(node_count);
  for (const auto& chunk_info : chunk_infos) {
    node_temperature.at(chunk_info.node) += chunk_info.temperature;
  }
  return scale(node_temperature);
}

int get_node_id(const PolymorphicAllocator<size_t>& alloc) {
  const auto memsource = dynamic_cast<NUMAMemoryResource*>(alloc.resource());
  if (memsource) {
    return memsource->get_node_id();
  }
  return -1;
}

// TODO(normanrz): Comment
std::vector<ChunkInfo> find_hot_chunks(const StorageManager& storage_manager, const std::chrono::milliseconds& lookback,
                                       const std::chrono::milliseconds& counter_history_interval) {
  std::vector<ChunkInfo> chunk_infos;
  double sum_temperature = 0.0;
  size_t lookback_samples = lookback.count() / counter_history_interval.count();
  for (const auto& table_name : storage_manager.table_names()) {
    const auto& table = *storage_manager.get_table(table_name);
    const auto chunk_count = table.chunk_count();
    for (ChunkID i = ChunkID(0); i < chunk_count; i++) {
      const auto& chunk = table.get_chunk(i);
      if (ChunkMigrationTask::chunk_is_completed(chunk, table.chunk_size()) && chunk.has_access_counter()) {
        const double temperature = static_cast<double>(chunk.access_counter()->history_sample(lookback_samples));
        sum_temperature += temperature;
        chunk_infos.emplace_back(ChunkInfo{/* .table_name = */ table_name,
                                           /* .id = */ i,
                                           /* .node = */ get_node_id(chunk.get_allocator()),
                                           /* .byte_size = */ chunk.byte_size(),
                                           /* .temperature = */ temperature});
      }
    }
  }
  std::sort(chunk_infos.begin(), chunk_infos.end(), [](const ChunkInfo& a, const ChunkInfo& b) { return b < a; });
  return chunk_infos;
}

// TODO(normanrz): Remove
std::vector<size_t> count_chunks_by_node(const std::vector<ChunkInfo>& chunk_infos, size_t node_count) {
  std::vector<size_t> result(node_count);
  for (const auto chunk_info : chunk_infos) {
    result.at(chunk_info.node)++;
  }
  return result;
}

MigrationPreparationTask::MigrationPreparationTask(const NUMAPlacementManager::Options& options) : _options(options) {}

// TODO(normanrz): Comment
void MigrationPreparationTask::_on_execute() {
  const auto topology = std::dynamic_pointer_cast<NodeQueueScheduler>(CurrentScheduler::get())->topology();

  auto hot_chunks =
      find_hot_chunks(StorageManager::get(), _options.counter_history_range, _options.counter_history_interval);
  size_t chunk_counter = 0;
  NodeInfoSet hot_nodes = find_hot_nodes(get_node_temperature(hot_chunks, topology->nodes().size()));

  // TODO(normanrz): Remove
  std::cout << "Imbalance: " << hot_nodes.imbalance << std::endl;
  print_vector(hot_nodes.node_temperature, "Hotnesses: ");
  print_vector(count_chunks_by_node(hot_chunks, topology->nodes().size()), "Chunk counts: ");

  if (hot_nodes.imbalance > _options.imbalance_threshold && hot_nodes.cold_nodes.size() > 0) {
    std::vector<ChunkInfo> migration_candidates;
    for (const auto& hot_chunk : hot_chunks) {
      if (hot_chunk.node < 0 || contains(hot_nodes.hot_nodes, static_cast<NodeID>(hot_chunk.node))) {
        migration_candidates.push_back(hot_chunk);
      }
      if (migration_candidates.size() >= _options.migration_count) {
        break;
      }
    }

    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    jobs.reserve(migration_candidates.size());

    for (const auto& migration_chunk : migration_candidates) {
      const auto target_node = hot_nodes.cold_nodes.at(chunk_counter % hot_nodes.cold_nodes.size());
      const auto task = std::make_shared<ChunkMigrationTask>(migration_chunk.table_name,
                                                             std::vector<ChunkID>({migration_chunk.id}), target_node);
      // TODO(normanrz): Remove
      std::cout << "Migrating " << migration_chunk.table_name << " (" << migration_chunk.id << ") "
                << migration_chunk.node << " -> " << target_node << std::endl;
      task->schedule(target_node, SchedulePriority::Unstealable);
      jobs.push_back(task);
      chunk_counter++;
    }

    CurrentScheduler::wait_for_tasks(jobs);
  }
}

}  // namespace opossum

#endif
