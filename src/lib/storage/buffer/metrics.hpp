#pragma once

#include <atomic>

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

  // The number of allocation
  std::atomic_uint64_t num_allocs = 0;
  std::atomic_uint64_t num_deallocs = 0;

  // Tracks the number of bytes copied between the different regions, TODO: Maybe add a count
  std::atomic_uint64_t total_bytes_copied_from_ssd_to_dram = 0;
  std::atomic_uint64_t total_bytes_copied_from_ssd_to_numa = 0;
  std::atomic_uint64_t total_bytes_copied_from_numa_to_dram = 0;
  std::atomic_uint64_t total_bytes_copied_from_dram_to_numa = 0;
  std::atomic_uint64_t total_bytes_copied_from_dram_to_ssd = 0;
  std::atomic_uint64_t total_bytes_copied_from_numa_to_ssd = 0;

  std::atomic_uint64_t total_bytes_copied_to_ssd = 0;
  std::atomic_uint64_t total_bytes_copied_from_ssd = 0;

  // Track hits and misses on DRAM or Numa
  std::atomic_uint64_t total_hits = 0;
  std::atomic_uint64_t total_misses = 0;

  // Tracks pinning
  std::atomic_uint64_t total_pins = 0;
  std::atomic_uint64_t current_pins = 0;

  // Tracks the number of evictions
  std::atomic_uint64_t num_dram_eviction_queue_items_purged = 0;
  std::atomic_uint64_t num_dram_eviction_queue_adds = 0;
  std::atomic_uint64_t num_numa_eviction_queue_items_purged = 0;
  std::atomic_uint64_t num_numa_eviction_queue_adds = 0;
  std::atomic_uint64_t num_dram_evictions;
  std::atomic_uint64_t num_numa_evictions;

  // Number of madvice calls, TODO: track in more places
  std::atomic_uint64_t num_madvice_free_calls = 0;
};