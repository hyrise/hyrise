#pragma once

#include <memory>
#include <tuple>
#include "noncopyable.hpp"
#include "storage/buffer/frame.hpp"
#include "storage/buffer/memory_resource.hpp"
#include "storage/buffer/migration_policy.hpp"
#include "storage/buffer/ssd_region.hpp"
#include "storage/buffer/types.hpp"
#include "storage/buffer/volatile_region.hpp"
#include "utils/pausable_loop_thread.hpp"

namespace hyrise {

template <typename PointedType>
class BufferPtr;

// TODO: Calculate hotness of a values as the inverse of the size so that smaller pages are likely to be evicted
// TODO: Do not create persistent page directly, but only when it is needed

/**
 * TODO
*/
class BufferManager : public MemoryResource, public Noncopyable {
 public:
  /**
   * Metrics are storing count that happen during allocations and access of the buffer manager.
  */
  struct Metrics {
    // The current amount of bytes being allocated
    std::size_t current_bytes_used_dram = 0;  // TODO: Add Different one to signify max usage in pool vs allo/delloc
    std::size_t current_bytes_used_numa = 0;
    std::size_t total_allocated_bytes_dram = 0;
    std::size_t total_allocated_bytes_numa = 0;
    std::size_t total_unused_bytes_numa = 0;
    std::size_t total_unused_bytes_dram = 0;  // TODO: this becomes invalid with the monotonic buffer resource

    double internal_fragmentation_rate_dram() const {
      return (double)total_unused_bytes_dram / (double)total_allocated_bytes_dram;
    }

    double internal_fragmentation_rate_numa() const {
      return (double)total_unused_bytes_numa / (double)total_allocated_bytes_numa;
    }

    // The number of allocation
    std::size_t num_allocs = 0;
    std::size_t num_deallocs = 0;

    // Tracks the number of bytes copied between the different regions, TODO: Maybe add a count
    std::size_t total_bytes_copied_from_ssd_to_dram = 0;
    std::size_t total_bytes_copied_from_ssd_to_numa = 0;
    std::size_t total_bytes_copied_from_numa_to_dram = 0;
    std::size_t total_bytes_copied_from_dram_to_numa = 0;
    std::size_t total_bytes_copied_from_dram_to_ssd = 0;
    std::size_t total_bytes_copied_from_numa_to_ssd = 0;
    std::size_t total_bytes_copied_to_ssd = 0;
    std::size_t total_bytes_copied_from_ssd = 0;

    // Track hits and misses on DRAM or Numa
    std::size_t total_hits_dram = 0;
    std::size_t total_hits_numa = 0;
    std::size_t total_misses_dram = 0;
    std::size_t total_misses_numa = 0;

    // Tracks pinning
    std::size_t total_pins_dram = 0;
    std::size_t current_pins_dram = 0;
    std::size_t total_pins_numa = 0;
    std::size_t current_pins_numa = 0;

    // Tracks the number of evictions
    std::size_t num_dram_eviction_queue_items_purged = 0;
    std::size_t num_dram_eviction_queue_adds = 0;
    std::size_t num_numa_eviction_queue_items_purged = 0;
    std::size_t num_numa_eviction_queue_adds = 0;
    std::size_t num_dram_evictions;
    std::size_t num_numa_evictions;

    // TODO: Ratio is defined in Spitfire paper. Lower values signfies lower duplication.
    // std::size_t dram_numa_inclusivity_ratio = 0;

    // Number of madvice calls, TODO: track in more places
    std::size_t num_madvice_free_calls_numa = 0;
    std::size_t num_madvice_free_calls_dram = 0;
  };

  struct Config {
    std::size_t dram_buffer_pool_size = 1UL << 30;  // 1 GB
    std::size_t numa_buffer_pool_size = 1UL << 34;  // 16 GB

    MigrationPolicy migration_policy = EagerMigrationPolicy{};

    int8_t numa_memory_node = NO_NUMA_MEMORY_NODE;

    std::filesystem::path ssd_path = "~/.hyrise";

    bool enable_eviction_purge_worker = false;

    BufferManagerMode mode = BufferManagerMode::DramSSD;

    static Config from_env();
  };

  BufferManager();

  BufferManager(const Config config);

  /**
   * @brief Pin a page marks a page unavailable for replacement. It needs to be unpinned before it can be replaced.
   * 
   * @param page_id 
   */
  void pin(std::shared_ptr<Frame> frame);

  /**
   * @brief Unpinning a page marks a page available for replacement. This acts as a soft-release without flushing
   * the page back to disk. Calls callback if the pin count is redced to zero.
   * 
   * @param page_id 
   */
  void unpin(std::shared_ptr<Frame> frame, const bool dirty = false);

  /**
   * @brief Loads a page from disk into the buffer manager. If the page is already in the buffer manager, it might be migrated. 
   * The returned pointer does not necessarily equal the pointer passed in.
  */
  void make_resident(const std::shared_ptr<Frame>& frame);

  std::shared_ptr<Frame>& load_frame(const std::shared_ptr<SharedFrame>& frame, const AccessIntent access_intent);

  /**
   * @brief Get the frame and offset for a given pointer.
   * 
   * @param ptr 
   * @return std::pair<std::weak_ptr<Frame>, std::ptrdiff_t> Pointer to the frame and the offset in the frame
   */
  std::pair<std::shared_ptr<Frame>, std::ptrdiff_t> unswizzle(const void* ptr);

  /**
   * Allocates pages to fullfil allocation request of the given bytes and alignment 
  */
  BufferPtr<void> allocate(std::size_t bytes, std::size_t align = alignof(std::max_align_t));

  /**
   * Deallocates a pointer and frees the pages.
  */
  void deallocate(BufferPtr<void> ptr, std::size_t bytes, std::size_t align = alignof(std::max_align_t));

  /**
   * @brief Returns a snapshot of metrics holding information about allocations, page table hits etc. of the current buffer manager instance.
  */
  Metrics metrics();

  /**
   * @brief Reset the metrics of the buffer manager e.g. when starting a benchmark run.
  */
  void reset_metrics();

  BufferManager& operator=(BufferManager&& other);

  /**
   * Get the current number of bytes used by the buffer manager on NUMA.
  */
  std::size_t numa_bytes_used() const;

  /**
   * Get the current number of bytes used by the buffer manager on DRAM.
  */
  std::size_t dram_bytes_used() const;

  /**
   * Get the current config object.
  */
  Config get_config() const;

  /**
   * Reset all data in the internal data structures. TODO: Move to private
  */
  void clear();

 protected:
  friend class Hyrise;
  friend class BufferPtrTest;
  friend class BufferManagerTest;
  friend class BufferPoolAllocatorTest;
  friend class PinGuardTest;
  friend class Frame;

 private:
  /**
   * Holds multiple sized buffer pools on either DRAM or NUMA memory.
  */
  // TODO: Introduce page type as template parameter
  struct BufferPools {
    void allocate_frame(std::shared_ptr<Frame> frame);
    void deallocate_frame(std::shared_ptr<Frame> frame);

    // Purge the eviction queue
    void purge_eviction_queue();

    // Add a page to the eviction queue
    void add_to_eviction_queue(std::shared_ptr<Frame> frame);

    void clear();

    BufferPools(const PageType page_type, const size_t pool_size,
                const std::function<void(const std::shared_ptr<Frame>&)> evict_frame,
                const bool enable_eviction_purge_worker, const std::shared_ptr<BufferManager::Metrics> metrics);

    BufferPools& operator=(BufferPools&& other);

    // The maximum number of bytes that can be allocated
    size_t _max_bytes;  // TODO: const

    // The number of bytes that are currently used
    std::atomic_uint64_t _used_bytes;

    // Memory Regions for each page size type
    std::array<std::unique_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> _buffer_pools;

    // Eviction Queue for pages that are not pinned
    std::shared_ptr<EvictionQueue> _eviction_queue;

    // Async background worker that purges the eviction queue
    std::unique_ptr<PausableLoopThread> _eviction_purge_worker;

    // Metrics passed down from buffer manager
    std::shared_ptr<BufferManager::Metrics> _metrics;

    // Current types of pages in the buffer pools
    PageType _page_type;

    // Enable or disable the buffer bool
    bool enabled;

    // Calls the parent buffer manager to evict a page using the migriation policy
    std::function<void(const std::shared_ptr<Frame>&)> _evict_frame;
  };

  // Allocate a new page in the buffer pool by allocating a frame. May evict a page from the buffer pool.
  std::shared_ptr<SharedFrame> new_frame(const PageSizeType size_type);

  // Evict a frame to a lower level buffer pool (NUMA) or to SSD
  void evict_frame(const std::shared_ptr<Frame>& frame);

  // Read a page from disk into the buffer pool
  void read_page_from_ssd(const std::shared_ptr<Frame>& frame);

  // Write out a page to disk
  void write_page_to_ssd(const std::shared_ptr<Frame>& frame);

  Config _config;  // TODO: Const;

  // Total number of pages currently in the buffer pool
  std::atomic_uint64_t _num_pages;

  // Metrics of buffer manager
  std::shared_ptr<Metrics> _metrics{};

  // Migration Policy to decide when to migrate pages to DRAM or NUMA buffer pools
  MigrationPolicy _migration_policy;

  // Memory Region for pages on SSD
  std::shared_ptr<SSDRegion> _ssd_region;  // TODO: Maybe this can become a unique_ptr

  // Memory Region for pages in DRAM using multiple volatile region for each page size type
  BufferPools _dram_buffer_pools;

  // Memory Region for pages on a NUMA nodes (preferrably memory only) using multiple volatile region for each page size type
  BufferPools _numa_buffer_pools;
};

}  // namespace hyrise