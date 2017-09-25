#pragma once

#include <numa.h>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <ostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "polymorphic_allocator.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "utils/fastrand.hpp"
#include "utils/numa_memory_resource.hpp"
#include "utils/pausable_loop_thread.hpp"

#define COUNTER_HISTORY_COUNT 100
#define COUNTER_HISTORY_INTERVAL std::chrono::milliseconds(100)

namespace opossum {

// The NUMAPlacementManager is a singleton that maintains the NUMA-aware allocators
class NUMAPlacementManager {
 public:
  static const std::shared_ptr<NUMAPlacementManager>& get();
  static void set(const std::shared_ptr<NUMAPlacementManager>& instance);
  static bool is_set();
  static int get_node_id_of(void* ptr);

  explicit NUMAPlacementManager(std::shared_ptr<Topology> topology);

  NUMAMemoryResource* get_memsource(int node_id);

  // template <typename T>
  // std::shared_ptr<pmr_vector<T>> migrate(const std::shared_ptr<pmr_vector<T>>& v, PolymorphicAllocator<T> alloc) {
  //   return std::allocate_shared<pmr_vector<T>>(alloc, v, alloc);
  // }

  // template <typename T>
  // std::shared_ptr<pmr_vector<T>> migrate(const std::shared_ptr<pmr_vector<T>>& v, size_t node_id) {
  //   const auto alloc = get_allocator<T>(node_id);
  //   return migrate<T>(v, alloc);
  // }

  // template <typename T>
  // pmr_vector<std::shared_ptr<T>> migrate(const pmr_vector<std::shared_ptr<T>>& v, PolymorphicAllocator<T> alloc) {
  //   pmr_vector<std::shared_ptr<T>> result(v.size(), alloc);
  //   for (size_t i = 0; i < v.size(); i++) {
  //     result.at(i) = std::allocate_shared<T>(alloc, v.at(i));
  //   }
  //   return result;
  // }

  const std::shared_ptr<Topology>& topology() const;

  void resume() {
    collector_thread->resume();
    migration_thread->resume();
  }
  void pause() {
    collector_thread->pause();
    migration_thread->resume();
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
