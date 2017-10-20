#pragma once
#if OPOSSUM_NUMA_SUPPORT

#include <chrono>
#include <memory>
#include <vector>
#include <experimental/memory_resource>

#include "polymorphic_allocator.hpp"
#include "scheduler/topology.hpp"

#include "utils/numa_memory_resource.hpp"
#include "utils/pausable_loop_thread.hpp"

namespace opossum {

class NUMAPlacementManager {}

struct NUMAPlacementManager::Options {
  // TODO(normanrz): Comment where these numbers come from and what they mean
  std::chrono::milliseconds counter_history_interval = std::chrono::milliseconds(100);
  std::chrono::milliseconds migration_interval = std::chrono::seconds(10);
  std::chrono::milliseconds counter_history_range = std::chrono::seconds(7);
  size_t migration_count = 3;
  double imbalance_threshold = 0.1;
};

// The NUMAPlacementManager is a singleton that maintains the NUMA-aware allocators
// and triggers the NUMA-aware chunk migration tasks
class NUMAPlacementManager {
 public:
  static const std::shared_ptr<NUMAPlacementManager>& get();
  static void set(const std::shared_ptr<NUMAPlacementManager>& instance);
  static bool is_set();
  static int get_node_id_of(void* ptr);

  explicit NUMAPlacementManager(const std::shared_ptr<Topology> topology,
                                const NUMAPlacementManager::Options options = NUMAPlacementManager::Options());

  const std::experimental::pmr::memory_resource* get_memory_resource(int node_id);

  const std::shared_ptr<Topology>& topology() const;

  void resume();
  void pause();

  NUMAPlacementManager(NUMAPlacementManager const&) = delete;
  NUMAPlacementManager& operator=(const NUMAPlacementManager&) = delete;
  NUMAPlacementManager(NUMAPlacementManager&&) = delete;

 protected:
  static std::shared_ptr<NUMAPlacementManager> _instance;

  const std::shared_ptr<Topology> _topology;
  const NUMAPlacementManager::Options _options;

  std::vector<NUMAMemoryResource> _memory_resources;
  std::unique_ptr<PausableLoopThread> _collector_thread;
  std::unique_ptr<PausableLoopThread> _migration_thread;
};
}  // namespace opossum
#endif
