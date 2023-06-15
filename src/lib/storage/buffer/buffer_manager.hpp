#pragma once

#include <boost/container/pmr/memory_resource.hpp>
#include <memory>
#include <tuple>
#include "noncopyable.hpp"
#include "storage/buffer/frame.hpp"
#include "storage/buffer/migration_policy.hpp"
#include "storage/buffer/ssd_region.hpp"
#include "storage/buffer/types.hpp"
#include "storage/buffer/volatile_region.hpp"
#include "utils/pausable_loop_thread.hpp"

namespace hyrise {

class BufferManager : public boost::container::pmr::memory_resource, public Noncopyable {
 public:
  // TODO: Add
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
    NumaMemoryNode memory_node = NO_NUMA_MEMORY_NODE;

    // Path to the SSD storage. Can be a block device or a directory. (default: ~/.hyrise).
    std::filesystem::path ssd_path = "~/.hyrise";

    // Enables a background threads the cleans the eviction queue from junk (default: false)
    bool enable_eviction_purge_worker = false;

    // Defines the mode of the buffer manager (default: dram_ssd). See BufferManagerMode for more details.
    BufferManagerMode mode = BufferManagerMode::DramSSD;

    // Load the configuration from the environment
    static Config from_env();
  };

  BufferManager();

  BufferManager(const Config config);

  ~BufferManager();

  BufferManager& operator=(BufferManager&& other) noexcept;

  static BufferManager& get();

  void pin_for_write(const PageID page_id);

  void pin_for_read(const PageID page_id);

  void set_dirty(const PageID page_id);

  void unpin_for_write(const PageID page_id);

  void unpin_for_read(const PageID page_id);

  PageID find_page(const void* ptr) const;

  void* do_allocate(std::size_t bytes, std::size_t alignment) override;

  void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override;

  bool do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept override;

  std::shared_ptr<Metrics> metrics();

  size_t memory_consumption() const;

 protected:
  friend class Hyrise;

 private:
  struct BufferPool {
    BufferPool(const size_t pool_size, const bool enable_eviction_purge_worker,
               std::array<std::unique_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES>& volatile_regions,
               SSDRegion* ssd_region, BufferPool* target_buffer_pool,
               const NumaMemoryNode memory_node = DEFAULT_DRAM_NUMA_NODE);

    BufferPool& operator=(BufferPool&& other) noexcept;

    void free_pages(const PageSizeType size);

    void purge_eviction_queue();

    void add_to_eviction_queue(const PageID page_id, Frame* frame);

    // The maximum number of bytes that can be allocated TODO: make const
    size_t max_bytes;

    // The number of bytes that are currently used
    std::atomic<size_t> used_bytes;

    SSDRegion* ssd_region;
    BufferPool* target_buffer_pool;

    // Eviction queue for frames that are not pinned
    std::unique_ptr<EvictionQueue> eviction_queue;

    // Async background worker that purges the eviction queue
    std::unique_ptr<PausableLoopThread> eviction_purge_worker;

    std::array<std::unique_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES>& volatile_regions;

    NumaMemoryNode memory_node;
  };

  VolatileRegion& get_region(const PageID page_id);

  void write_page(const PageID page_id);

  void read_page(const PageID page_id);

  void make_resident(const PageID page_id, const Frame* frame, const AccessIntent access_intent);

  void add_to_eviction_queue(const PageID page_id, Frame* frame);

  Config _config;  // TODO: Const

  std::byte* _mapped_region;

  std::array<std::unique_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> _volatile_regions;

  BufferPool _secondary_buffer_pool;

  BufferPool _primary_buffer_pool;

  SSDRegion _ssd_region;

  std::shared_ptr<Metrics> _metrics;
};
}  // namespace hyrise