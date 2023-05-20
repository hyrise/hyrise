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

// TODO: Calculate hotness of a values as the inverse of the size so that smaller pages are likely to be evicted
// TODO: Do not create persistent page directly, but only when it is needed
// TODO: Introduce page type as template parameter to buffer pools

class BufferManager : public MemoryResource, public Noncopyable {
 public:
  // Metrics are storing statistics like copied bytes that happen during allocations and access of the buffer manager.
  struct Metrics {
    // The current amount of bytes being allocated
    std::atomic_uint64_t current_bytes_used_dram =
        0;  // TODO: Add Different one to signify max usage in pool vs allo/delloc
    std::atomic_uint64_t current_bytes_used_numa = 0;
    std::atomic_uint64_t total_allocated_bytes_dram = 0;
    std::atomic_uint64_t total_allocated_bytes_numa = 0;
    std::atomic_uint64_t total_unused_bytes_numa = 0;
    std::atomic_uint64_t total_unused_bytes_dram = 0;  // TODO: this becomes invalid with the monotonic buffer resource

    double internal_fragmentation_rate_dram() const {
      if (total_allocated_bytes_dram == 0) {
        return 0;
      }
      return (double)total_unused_bytes_dram / (double)total_allocated_bytes_dram;
    }

    double internal_fragmentation_rate_numa() const {
      if (total_allocated_bytes_numa == 0) {
        return 0;
      }
      return (double)total_unused_bytes_numa / (double)total_allocated_bytes_numa;
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
    std::atomic_uint64_t total_hits_dram = 0;
    std::atomic_uint64_t total_hits_numa = 0;
    std::atomic_uint64_t total_misses_dram = 0;
    std::atomic_uint64_t total_misses_numa = 0;

    // Tracks pinning
    std::atomic_uint64_t total_pins_dram = 0;
    std::atomic_uint64_t current_pins_dram = 0;
    std::atomic_uint64_t total_pins_numa = 0;
    std::atomic_uint64_t current_pins_numa = 0;

    // Tracks the number of evictions
    std::atomic_uint64_t num_dram_eviction_queue_items_purged = 0;
    std::atomic_uint64_t num_dram_eviction_queue_adds = 0;
    std::atomic_uint64_t num_numa_eviction_queue_items_purged = 0;
    std::atomic_uint64_t num_numa_eviction_queue_adds = 0;
    std::atomic_uint64_t num_dram_evictions;
    std::atomic_uint64_t num_numa_evictions;

    // TODO: Ratio is defined in Spitfire paper. Lower values signfies lower duplication.
    // std::size_t dram_numa_inclusivity_ratio = 0;

    // Number of madvice calls, TODO: track in more places
    std::atomic_uint64_t num_madvice_free_calls_numa = 0;
    std::atomic_uint64_t num_madvice_free_calls_dram = 0;
  };

  struct Config {
    // Defines the size of the buffer pool in DRAM in bytes (default: 1 GB)
    std::size_t dram_buffer_pool_size = 1UL << 30;

    // Defines the size of the buffer pool in DRAM in bytes (default: 16 GB)
    std::size_t numa_buffer_pool_size = 1UL << 34;

    // Defines the miration policy to use (default: eager). See MigrationPolicy for more details.
    MigrationPolicy migration_policy = EagerMigrationPolicy{};

    // Identifier of the NUMA node to use for the buffer pool (default: -1, i.e., no NUMA node)
    int8_t numa_memory_node = NO_NUMA_MEMORY_NODE;

    // Path to the SSD storage. Can be a block device or a directory. (default: ~/.hyrise).
    std::filesystem::path ssd_path = "~/.hyrise";

    // Enables a background threads the cleans the eviction queue from junk (default: false)
    bool enable_eviction_purge_worker = false;

    // Defines the mode of the buffer manager (default: dram_ssd). See BufferManagerMode for more details.
    BufferManagerMode mode = BufferManagerMode::DramSSD;

    // Load the configuration from the environment
    static Config from_env();
  };

  /**
   * @brief Construct a new Buffer Manager that reads the configuration from the environment.
  */
  BufferManager();

  /**
   * @brief Construct a new Buffer Manager with the given configuration.
   * @param config The configuration to use.
  */
  BufferManager(const Config config);

  /**
   * @brief Pininig a frame marks a frame locks it. It needs to be unpinned before it can be replaced by the eviction policy.
   * 
   * @param FramePtr Pointer to the frame to pin. 
   */
  void pin(const FramePtr& frame);

  /**
   * @brief Unpinning (pin count = 0) marks a frame available for replacement.
   * 
   * @param FramePtr Pointer to the frame to unpin.  
   * @param dirty If the page is dirty, it will be flushed back to a lower layer.
   */
  void unpin(const FramePtr& frame, const bool dirty = false);

  /**
   * @brief Load a frame or its sibling depending on the migration policy. The returned frame is not necessarily the same as the passed frame. 
   * The returned frame is guaranteed to be resident. It also likely to be unpinned in most cases and put into the back of the eviction queue. 
   * Therefore, immediate accesses should *likely*, but not guaranteed, yield valid data.
   * 
   * @param frame The frame to load.
   * @param access_intent The access intent for the frame. Could be Read or Write. This controls different probabilities given by the migration policy. 
  */
  FramePtr make_resident(FramePtr frame, const AccessIntent access_intent);

  /**
   * @brief Get the frame and offset for a given pointer.
   * 
   * @param ptr 
   * @return std::pair<FramePtr, std::ptrdiff_t> Pointer to the frame and the offset in the frame
   */
  std::pair<Frame*, std::ptrdiff_t> find_frame_and_offset(const void* ptr);

  /**
   * @brief Allocates pages to fullfil allocation request of the given bytes and alignment 
  */
  BufferPtr<void> allocate(std::size_t bytes, std::size_t align = alignof(std::max_align_t));

  /**
   * @brief Deallocates a pointer. 
  */
  void deallocate(BufferPtr<void> ptr, std::size_t bytes, std::size_t align = alignof(std::max_align_t));

  /**
   * @brief Returns the current metrics holding information about allocations, page table hits etc. of the current buffer manager instance.
  */
  std::shared_ptr<Metrics> metrics();

  /**
   * @brief Reset the metrics of the buffer manager e.g. when starting a benchmark run.
  */
  void reset_metrics();

  /**
   * @brief Copy assignment operator. Shouly be used carefully as it might leads to invalid data.
  */
  BufferManager& operator=(BufferManager&& other);

  /**
   *@brief  Get the current number of bytes used by the buffer manager on NUMA.
  */
  std::size_t numa_bytes_used() const;

  /**
   * @brief Get the current number of bytes used by the buffer manager on DRAM.
  */
  std::size_t dram_bytes_used() const;

  /**
   * @brief Get the current config object.
  */
  Config get_config() const;

  /**
  * @brief Reset all data in the internal data structures. Be careful with this as you still have data pointing to this buffer manager.
  */
  void clear();

  /**
   * @brief Get the current memory consumption of the buffer manager in bytes for all its data structures.
  */
  size_t memory_consumption() const;

  /**
   * @brief Dummy Frame is a null object of a frame. It is used by the BufferPtr if the pointer is outside of the buffer manager.
  */
  Frame DUMMY_FRAME = Frame(PageID{INVALID_PAGE_ID}, PageSizeType::KiB8, PageType::Invalid, nullptr);

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
  struct BufferPools {
    void allocate_frame(FramePtr& frame);
    void deallocate_frame(FramePtr& frame);

    // Purge the eviction queue
    void purge_eviction_queue();

    // Add a frame to the eviction queue
    void add_to_eviction_queue(const FramePtr& frame);

    // Soft-clears the buffer pools
    void clear();

    BufferPools(const PageType page_type, const size_t pool_size,
                const std::function<void(const FramePtr&)> evict_frame, const bool enable_eviction_purge_worker,
                const std::shared_ptr<BufferManager::Metrics> metrics,
                const size_t numa_memory_node = NO_NUMA_MEMORY_NODE);
    ~BufferPools();

    BufferPools& operator=(BufferPools&& other);

    // The maximum number of bytes that can be allocated TODO: make const
    size_t _max_bytes;

    // The number of bytes that are currently used
    std::atomic_uint64_t _used_bytes;

    // Memory Regions for each page size type
    std::array<std::unique_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> _buffer_pools;

    // Eviction queue for frames that are not pinned
    std::shared_ptr<EvictionQueue> _eviction_queue;

    // Async background worker that purges the eviction queue
    std::unique_ptr<PausableLoopThread> _eviction_purge_worker;

    // Metrics passed down from buffer manager
    std::shared_ptr<BufferManager::Metrics> _metrics;

    // Current types of pages in the buffer pools
    PageType _page_type;

    // Enable or disable the buffer bool
    bool enabled;

    // Calls the parent buffer manager to evict a frame using the migration policy
    std::function<void(const FramePtr&)> _evict_frame;

    // Returns the frame the data pointer is pointing to and the offset in the frame
    std::pair<Frame*, std::ptrdiff_t> ptr_to_frame(const void* ptr);

    // Returns how much memory is consumed by the state of the buffer pools
    size_t memory_consumption() const;
  };

  // Allocate a new frame in the buffer pool by allocating a frame. May evict a frame from the buffer pool.
  FramePtr new_frame(const PageSizeType size_type);

  // Evict a frame to a lower level buffer pool (NUMA) or to SSD
  void evict_frame(const FramePtr& frame);

  // Read a frame from disk into the buffer pool
  void read_page_from_ssd(const FramePtr& frame);

  // Write out a frame to disk
  void write_page_to_ssd(const FramePtr& frame);

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