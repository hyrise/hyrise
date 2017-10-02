#pragma once

#include <memory>

#include "polymorphic_allocator.hpp"
#include "scheduler/topology.hpp"
#include "utils/numa_memory_resource.hpp"
#include "utils/pausable_loop_thread.hpp"

namespace opossum {

// The NUMAPlacementManager is a singleton that maintains the NUMA-aware allocators
// and triggers the NUMA-aware chunk migration tasks
class NUMAPlacementManager {
 public:
  static const std::shared_ptr<NUMAPlacementManager>& get();
  static void set(const std::shared_ptr<NUMAPlacementManager>& instance);
  static bool is_set();
  static int get_node_id_of(void* ptr);

  explicit NUMAPlacementManager(std::shared_ptr<Topology> topology);

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
  pmr_vector<NUMAMemoryResource> memsources;
  size_t node_counter = 0;

  std::unique_ptr<PausableLoopThread> collector_thread;
  std::unique_ptr<PausableLoopThread> migration_thread;
};
}  // namespace opossum
