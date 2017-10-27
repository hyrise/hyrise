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
class NUMAPlacementManager {
 public:
  struct Options {
    // TODO(normanrz): Comment where these numbers come from and what they mean
    std::chrono::milliseconds counter_history_interval = std::chrono::milliseconds(100);
    std::chrono::milliseconds migration_interval = std::chrono::seconds(10);
    std::chrono::milliseconds counter_history_range = std::chrono::seconds(7);
    size_t migration_count = 3;
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
