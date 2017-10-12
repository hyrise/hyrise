#pragma once

#include <chrono>
#include <memory>
#include <vector>

#include "polymorphic_allocator.hpp"
#include "scheduler/topology.hpp"
#include "utils/numa_memory_resource.hpp"
#include "utils/pausable_loop_thread.hpp"

namespace opossum {

struct NUMAPlacementManagerOptions {
  std::chrono::milliseconds counter_history_interval = std::chrono::milliseconds(100);
  std::chrono::milliseconds migration_interval = std::chrono::milliseconds(10);
  std::chrono::milliseconds counter_history_range = std::chrono::milliseconds(7);
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

  explicit NUMAPlacementManager(std::shared_ptr<Topology> topology,
                                const NUMAPlacementManagerOptions options = NUMAPlacementManagerOptions());

  NUMAMemoryResource* get_memsource(int node_id);

  const std::shared_ptr<Topology>& topology() const;

  void resume() {
    collector_thread->resume();
    migration_thread->resume();
  }
  void pause() {
    collector_thread->pause();
    migration_thread->pause();
  }

  NUMAPlacementManager(NUMAPlacementManager const&) = delete;
  NUMAPlacementManager& operator=(const NUMAPlacementManager&) = delete;
  NUMAPlacementManager(NUMAPlacementManager&&) = delete;

 protected:
  static std::shared_ptr<NUMAPlacementManager> _instance;

  std::shared_ptr<Topology> _topology;
  std::vector<NUMAMemoryResource> memsources;

  const NUMAPlacementManagerOptions _options;

  std::unique_ptr<PausableLoopThread> collector_thread;
  std::unique_ptr<PausableLoopThread> migration_thread;
};
}  // namespace opossum
