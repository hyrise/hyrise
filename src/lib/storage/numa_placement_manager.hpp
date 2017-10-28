#pragma once

#if HYRISE_NUMA_SUPPORT

#include <boost/container/pmr/memory_resource.hpp>

#include <chrono>
#include <memory>
#include <vector>

#include "scheduler/topology.hpp"
#include "utils/numa_memory_resource.hpp"
#include "utils/pausable_loop_thread.hpp"

namespace opossum {

// The NUMAPlacementManager is a singleton that maintains the NUMA-aware allocators
// and triggers the NUMA-aware chunk migration tasks
// The NUMAPlacementManager is initialized in a paused state and needs to be 
// `resumed` to start its operation.
class NUMAPlacementManager {
 public:
  struct Options {
    // Parameters of the NUMA placement and chunk migration algorithm.
    // These parameters have been obtained from running experiments with dynamic
    // workloads in the database. The workloads in the experiments changed after
    // 2min 30s. These parameters should be adjusted for when workloads change 
    // more or less frequently.

    // The time interval at which a snaphsot of the current access time counters
    // of all stored chunks are preserved. These access times are the basis to 
    // detect load imbalances between the NUMA nodes. This procedure is run in a loop. 
    std::chrono::milliseconds counter_history_interval = std::chrono::milliseconds(100);

    // The time interval at which the migration routine (MigrationPreparationTask) 
    // is scheduled in a loop.
    // Should be much higher than counter_history_interval
    std::chrono::milliseconds migration_interval = std::chrono::seconds(10);

    // The time interval of what historical chunk access times are considered 
    // for determining the load imbalance between the NUMA nodes.
    // Should be set less than 100 * counter_history_interval and less than
    // migration_interval
    std::chrono::milliseconds counter_history_range = std::chrono::seconds(7);

    // Number of chunks that are migrated per loop iteration
    size_t migration_count = 3;

    // The threshold for the load imbalance metric between the NUMA nodes.
    // If the imbalance is less than this threshold, no chunks are migrated.
    double imbalance_threshold = 0.1;
  };

  static NUMAPlacementManager& get();
  static int get_node_id_of(void* ptr);

  boost::container::pmr::memory_resource* get_memory_resource(int node_id);

  const std::shared_ptr<Topology>& topology() const;

  void resume();
  void pause();

  NUMAPlacementManager(NUMAPlacementManager const&) = delete;
  NUMAPlacementManager(NUMAPlacementManager&&) = delete;

 protected:
  explicit NUMAPlacementManager(const std::shared_ptr<Topology> topology, const Options options);

  const std::shared_ptr<Topology> _topology;
  const Options _options;

  std::vector<NUMAMemoryResource> _memory_resources;
  std::unique_ptr<PausableLoopThread> _collector_thread;
  std::unique_ptr<PausableLoopThread> _migration_thread;

  NUMAPlacementManager& operator=(const NUMAPlacementManager&) = default;
};
}  // namespace opossum
#endif
