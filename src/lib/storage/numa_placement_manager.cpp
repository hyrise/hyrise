#if HYRISE_NUMA_SUPPORT

#include "numa_placement_manager.hpp"

#include <numa.h>
#include <boost/container/pmr/memory_resource.hpp>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>

#include "tasks/chunk_metrics_collection_task.hpp"
#include "tasks/migration_preparation_task.hpp"
#include "utils/assert.hpp"

namespace opossum {

// singleton
NUMAPlacementManager& NUMAPlacementManager::get() {
  static NUMAPlacementManager instance;
  return instance;
}

void NUMAPlacementManager::reset(const Options options, const std::shared_ptr<Topology> topology) {
  get() = NUMAPlacementManager(options, topology);
}

NUMAPlacementManager::NUMAPlacementManager(const NUMAPlacementManager::Options options,
                                           const std::shared_ptr<Topology> topology)
    : _options(options), _topology(topology) {
  for (size_t i = 0; i < _topology->nodes().size(); i++) {
    char msource_name[26];
    std::snprintf(msource_name, sizeof(msource_name), "numa_%03lu", i);
    _memory_resources.push_back(NUMAMemoryResource(i, std::string(msource_name)));
  }

  _collector_thread = std::make_unique<PausableLoopThread>(_options.counter_history_interval,
                                                           [](size_t) { ChunkMetricsCollectionTask().execute(); });

  _migration_thread = std::make_unique<PausableLoopThread>(
      _options.migration_interval, [options](size_t) { MigrationPreparationTask(options).execute(); });
}

boost::container::pmr::memory_resource* NUMAPlacementManager::get_memory_resource(int node_id) {
  DebugAssert(node_id >= 0 && node_id < static_cast<int>(_topology->nodes().size()), "node_id is out of bounds");
  return &_memory_resources[static_cast<size_t>(node_id)];
}

int NUMAPlacementManager::get_node_id_of(void* ptr) {
  int status[1];
  void* addr = {ptr};
  numa_move_pages(0, 1, static_cast<void**>(&addr), NULL, reinterpret_cast<int*>(&status), 0);
  return status[0];
}

void NUMAPlacementManager::resume() {
  _collector_thread->resume();
  _migration_thread->resume();
}
void NUMAPlacementManager::pause() {
  _collector_thread->pause();
  _migration_thread->pause();
}

const NUMAPlacementManager::Options& NUMAPlacementManager::options() const { return _options; }

const std::shared_ptr<Topology>& NUMAPlacementManager::topology() const { return _topology; }

}  // namespace opossum

#else

int numa_placement_manager_dummy;

#endif
