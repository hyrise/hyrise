#pragma once

#if HYRISE_NUMA_SUPPORT

#include <chrono>
#include <memory>
#include <vector>

#include "utils/pausable_loop_thread.hpp"
#include "utils/singleton.hpp"

namespace boost {
namespace container {
namespace pmr {
class memory_resource;
}
}  // namespace container
}  // namespace boost

namespace opossum {

// The NUMAPlacementManager is a singleton that maintains the NUMA-aware allocators
// and triggers the NUMA-aware chunk migration tasks
// The NUMAPlacementManager is initialized in a paused state and needs to be
// `resumed` to start its operation.

// Don't even think about writing a reset method. If the NUMAPlacementManager gets deleted, so do the memory
// resources. Without those, all destructors of all PMR vectors out there will fail.
class NUMAPlacementManager : public Singleton<NUMAPlacementManager> {
 public:
  struct Options {
    // Parameters of the NUMA placement and chunk migration algorithm.

    // These default parameters have been obtained from running experiments with
    // dynamic workloads in the database. The workloads in the experiments changed
    // after 2min 30s. These parameters should be adjusted for when workloads
    // change more or less frequently.
    Options()
        : counter_history_interval(std::chrono::milliseconds(100)),
          migration_interval(std::chrono::seconds(10)),
          counter_history_range(std::chrono::seconds(7)),
          migration_count(3),
          imbalance_threshold(0.1) {}

    // The time interval at which a snaphsot of the current access time counters
    // of all stored chunks are preserved. These access times are the basis to
    // detect load imbalances between the NUMA nodes. This procedure is run in a loop.
    std::chrono::milliseconds counter_history_interval;

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

  // Returns the memory resource of the next node according to a round robin placement policy
  boost::container::pmr::memory_resource* get_next_memory_resource();

  const Options& options() const;

  void resume();
  void pause();

  void set_options(const Options options);

  NUMAPlacementManager(NUMAPlacementManager&&) = delete;

 protected:
  NUMAPlacementManager();

  friend class Singleton;

  Options _options;
  int _current_node_id;

  std::unique_ptr<PausableLoopThread> _collector_thread;
  std::unique_ptr<PausableLoopThread> _migration_thread;

  NUMAPlacementManager& operator=(const NUMAPlacementManager&) = default;
  NUMAPlacementManager& operator=(NUMAPlacementManager&&) = default;
};
}  // namespace opossum
#endif
