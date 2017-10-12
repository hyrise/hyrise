#include "numa_placement_manager.hpp"

#include <numa.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>

#include "tasks/chunk_metrics_collection_task.hpp"
#include "tasks/migration_preparation_task.hpp"

namespace opossum {

std::shared_ptr<NUMAPlacementManager> NUMAPlacementManager::_instance;

const std::shared_ptr<NUMAPlacementManager>& NUMAPlacementManager::get() { return _instance; }

void NUMAPlacementManager::set(const std::shared_ptr<NUMAPlacementManager>& instance) { _instance = instance; }

bool NUMAPlacementManager::is_set() { return !!_instance; }

NUMAPlacementManager::NUMAPlacementManager(std::shared_ptr<Topology> topology,
                                           const NUMAPlacementManagerOptions options)
    : _topology(topology), _options(options) {
  for (size_t i = 0; i < _topology->nodes().size(); i++) {
    char msource_name[8];
    std::snprintf(msource_name, sizeof(msource_name), "numa_%03lu", i);
    memsources.push_back(NUMAMemoryResource(i, std::string(msource_name)));
  }

  collector_thread = std::make_unique<PausableLoopThread>(_options.counter_history_interval, [](size_t) {
    const auto task = std::make_shared<ChunkMetricsCollectionTask>();
    task->schedule();
    task->join();
  });

  migration_thread = std::make_unique<PausableLoopThread>(_options.migration_interval, [this](size_t) {
    const auto task = std::make_shared<MigrationPreparationTask>(_options);
    task->schedule();
    task->join();
  });
}

NUMAMemoryResource* NUMAPlacementManager::get_memsource(int node_id) {
  if (node_id < 0 || node_id >= static_cast<int>(_topology->nodes().size())) {
    throw std::range_error("node_id is out of bounds");
  }
  return &memsources.at(static_cast<size_t>(node_id));
}

int NUMAPlacementManager::get_node_id_of(void* ptr) {
  int status[1];
  void* addr = {ptr};
  numa_move_pages(0, 1, static_cast<void**>(&addr), NULL, reinterpret_cast<int*>(&status), 0);
  return status[0];
}

const std::shared_ptr<Topology>& NUMAPlacementManager::topology() const { return _topology; }

}  // namespace opossum
