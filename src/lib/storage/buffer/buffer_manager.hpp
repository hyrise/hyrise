#pragma once

#include <boost/container/pmr/memory_resource.hpp>
#include <memory>
#include <nlohmann/json.hpp>
#include <tuple>
#include "noncopyable.hpp"
#include "storage/buffer/frame.hpp"
#include "storage/buffer/metrics.hpp"
#include "storage/buffer/migration_policy.hpp"
#include "storage/buffer/ssd_region.hpp"
#include "storage/buffer/types.hpp"
#include "storage/buffer/volatile_region.hpp"
#include "utils/pausable_loop_thread.hpp"

namespace hyrise {

std::byte* create_mapped_region();
std::array<std::shared_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> create_volatile_regions(std::byte* mapped_region);
void unmap_region(std::byte* region);

class BufferManager : public boost::container::pmr::memory_resource, public Noncopyable {
 public:
  struct Config {
    // Defines the size of the buffer pool in DRAM in bytes (default: 1 GB)
    std::size_t dram_buffer_pool_size = 1UL << 30;

    // Defines the size of the buffer pool in DRAM in bytes (default: 16 GB)
    std::size_t numa_buffer_pool_size = 1UL << 34;

    // Defines the miration policy to use (default: eager). See MigrationPolicy for more details.
    MigrationPolicy migration_policy = LazyMigrationPolicy;

    // Identifier of the NUMA node to use for the buffer pool (default: -1, i.e., no NUMA node)
    NumaMemoryNode memory_node = NO_NUMA_MEMORY_NODE;

    // Path to the SSD storage. Can be a block device or a directory. (default: ~/.hyrise).
    std::filesystem::path ssd_path = "~/.hyrise";

    // Enables a background threads the cleans the eviction queue from junk (default: false)
    bool enable_eviction_purge_worker = false;

    // Load the configuration from the environment
    static Config from_env();

    nlohmann::json to_json() const;
  };

  BufferManager();

  BufferManager(const Config config);

  ~BufferManager() override;

  BufferManager& operator=(BufferManager&& other) noexcept;

  static BufferManager& get();

  // Rename to shared and exclusive lock
  void pin_exclusive(const PageID page_id);

  void pin_shared(const PageID page_id, const AccessIntent accessIntent);

  void unpin_exclusive(const PageID page_id);

  void unpin_shared(const PageID page_id);

  void set_dirty(const PageID page_id);

  PageID find_page(const void* ptr) const;

  void* do_allocate(std::size_t bytes, std::size_t alignment) override;

  void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override;

  bool do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept override;

  Config config() const;

  // Metrics and stats
  std::shared_ptr<BufferManagerMetrics> metrics();

  size_t memory_consumption() const;
  size_t reserved_bytes_dram_buffer_pool() const;
  size_t reserved_bytes_numa_buffer_pool() const;
  size_t free_bytes_dram_node() const;
  size_t free_bytes_numa_node() const;
  size_t total_bytes_dram_node() const;
  size_t total_bytes_numa_node() const;

  // Debugging methods
  StateVersionType _state(const PageID page_id);
  std::byte* _get_page_ptr(const PageID page_id);

 protected:
  friend class Hyrise;

 private:
  struct BufferPool {
    BufferPool(const size_t pool_size, const bool enable_eviction_purge_worker,
               std::array<std::shared_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> volatile_regions,
               MigrationPolicy migration_policy, std::shared_ptr<SSDRegion> ssd_region,
               std::shared_ptr<BufferPool> target_buffer_pool, std::shared_ptr<BufferManagerMetrics> metrics,
               const NumaMemoryNode memory_node = DEFAULT_DRAM_NUMA_NODE);

    void evict(EvictionItem& item, Frame* frame);

    void release_page(const PageSizeType size);

    bool ensure_free_pages(const PageSizeType size);

    void purge_eviction_queue();

    void add_to_eviction_queue(const PageID page_id, Frame* frame);

    bool enabled() const;

    size_t free_bytes_node() const;

    size_t total_bytes_node() const;

    size_t memory_consumption() const;

    // The maximum number of bytes that can be allocated
    const size_t max_bytes;

    // The number of bytes that are currently used
    std::atomic<size_t> used_bytes;

    std::shared_ptr<BufferManagerMetrics> metrics;

    std::shared_ptr<SSDRegion> ssd_region;

    std::shared_ptr<BufferPool> target_buffer_pool;

    // Eviction queue for frames that are not pinned
    std::unique_ptr<EvictionQueue> eviction_queue;

    // Async background worker that purges the eviction queue
    std::unique_ptr<PausableLoopThread> eviction_purge_worker;

    const MigrationPolicy migration_policy;

    std::array<std::shared_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> volatile_regions;

    const NumaMemoryNode memory_node;
  };

  std::shared_ptr<VolatileRegion> get_region(const PageID page_id);

  void protect_page(const PageID page_id);

  void unprotect_page(const PageID page_id);

  void make_resident(const PageID page_id, const AccessIntent access_intent,
                     const StateVersionType previous_state_version);

  void add_to_eviction_queue(const PageID page_id, Frame* frame);

  Config _config;  // TODO: Const

  std::byte* _mapped_region;

  std::shared_ptr<BufferManagerMetrics> _metrics;

  std::array<std::shared_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> _volatile_regions;

  std::shared_ptr<SSDRegion> _ssd_region;

  std::shared_ptr<BufferPool> _secondary_buffer_pool;

  std::shared_ptr<BufferPool> _primary_buffer_pool;
};

template <typename T>
inline T& DebugAssertNotEvictedAndReturn(T& value) {
  auto page_id = BufferManager::get().find_page(value.data());
  // DebugAssert(page_id.valid(), "Accessing invalid page");
  // auto state = BufferManager::get()._state(page_id);
  // DebugAssert(state == Frame::LOCKED || (state > Frame::UNLOCKED && state <= Frame::LOCKED_SHARED),
  //             "Accessing non-pinned value: " + std::to_string(state));
  return value;
}

}  // namespace hyrise