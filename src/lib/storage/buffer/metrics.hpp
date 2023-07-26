#pragma once

#include <atomic>
#include "storage/buffer/buffer_pool.hpp"

namespace hyrise {

// TODO: Add
struct BufferManagerMetrics {
  // The current amount of bytes being allocated
  //   std::atomic_uint64_t current_bytes_used_dram =
  //       0;  // TODO: Add Different one to signify max usage in pool vs allo/delloc
  //   std::atomic_uint64_t current_bytes_used_numa = 0;

  std::atomic_uint64_t total_allocated_bytes;
  std::atomic_uint64_t total_unused_bytes = 0;  // TODO: this becomes invalid with the monotonic buffer resource

  double internal_fragmentation_rate() const {
    if (total_allocated_bytes == 0) {
      return 0;
    }
    return (double)total_unused_bytes / (double)total_allocated_bytes;
  }

  double hit_rate() const {
    if (total_hits + total_misses == 0) {
      return 0;
    }
    return (double)total_hits / (double)(total_hits + total_misses);
  }

  // The number of allocation
  std::atomic_uint64_t num_allocs = 0;
  std::atomic_uint64_t num_deallocs = 0;

  // Tracks the number of bytes copied between the different regions, TODO: Maybe add a count
  std::atomic_uint64_t total_bytes_copied_from_ssd_to_dram = 0;
  std::atomic_uint64_t total_bytes_copied_from_ssd_to_numa = 0;
  std::atomic_uint64_t total_bytes_copied_from_numa_to_dram = 0;
  std::atomic_uint64_t total_bytes_copied_from_dram_to_numa = 0;

  std::atomic_uint64_t total_bytes_copied_to_ssd = 0;
  std::atomic_uint64_t total_bytes_copied_from_ssd = 0;

  // Track hits and misses on DRAM or Numa
  std::atomic_uint64_t total_hits = 0;
  std::atomic_uint64_t total_misses = 0;

  // Tracks pinning
  std::atomic_uint64_t total_pins = 0;
  std::atomic_uint64_t current_pins = 0;

  // Number of madvice calls
  std::atomic_uint64_t num_numa_tonode_memory_calls = 0;
  std::atomic_uint64_t num_madvice_free_calls = 0;

  std::shared_ptr<BufferPool::Metrics> dram_buffer_pool_metrics;
  std::shared_ptr<BufferPool::Metrics> numa_buffer_pool_metrics;
};

inline void increment_counter(std::atomic_uint64_t& metric, const size_t update = 1) {
  metric.fetch_add(update, std::memory_order_relaxed);
}
}  // namespace hyrise