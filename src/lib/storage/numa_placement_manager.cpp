#if HYRISE_NUMA_SUPPORT

#include "numa_placement_manager.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>

#include "scheduler/topology.hpp"
#include "storage/storage_manager.hpp"
#include "tasks/chunk_metrics_collection_task.hpp"
#include "tasks/migration_preparation_task.hpp"
#include "utils/assert.hpp"

namespace opossum {

NUMAPlacementManager::NUMAPlacementManager() : _current_node_id(0) {
  // The NUMAPlacementManager must exist before any table is stored in the storage manager. Otherwise, we might migrate
  // parts of that table. On termination of the program, the NUMAPlacementManager would be destroyed first, taking the
  // memory sources with it. This means that the destructors of those tables would fail.
  Assert(StorageManager::get().tables().empty(), "NUMAPlacementManager must be created before any table");

  _collector_thread = std::make_unique<PausableLoopThread>(_options.counter_history_interval,
                                                           [](size_t) { ChunkMetricsCollectionTask().execute(); });

  _migration_thread = std::make_unique<PausableLoopThread>(
      _options.migration_interval, [this](size_t) { MigrationPreparationTask(_options).execute(); });
}

void NUMAPlacementManager::set_options(const NUMAPlacementManager::Options options) {
  _options = options;
  _collector_thread->set_loop_sleep_time(_options.counter_history_interval);
  _migration_thread->set_loop_sleep_time(_options.migration_interval);
}

boost::container::pmr::memory_resource* NUMAPlacementManager::get_next_memory_resource() {
  const auto node_id = _current_node_id;
  _current_node_id = (_current_node_id + 1) % Topology::get().nodes().size();
  return Topology::get().get_memory_resource(node_id);
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

}  // namespace opossum

#else

int numa_placement_manager_dummy;

#endif
