#pragma once

#include "concurrentqueue.h"
#include "persistence_manager.hpp"
#include "types.hpp"
#include "volatile_region.hpp"

namespace hyrise {

class SSDRegion;
class VolatileRegion;

/**
 * A buffer pool abstracts access to a set of memory on a NUMA node. It is responsible for eviction using a Second-Chance FIFO quque to another node or to the SSD
 * based on the migration policy. Most functions including the constructor are private to the buffer manager class. The exposed public functions can be used to 
 * gather statistics and metadata. The usage of the buffer pool is thread-safe except for the resizing.
 * 
 * The implementation is inspired by DuckDB (https://github.com/duckdb/duckdb) and Kuzu (https://github.com/kuzudb/kuzu).
*/
class BufferPool final {
 public:
  // Current NUMA node of the buffer pool
  NodeID node_id() const;

  // Current number of bbtes used by the buffer pool
  uint64_t reserved_bytes() const;

  // Limit of bytes that can be used by the buffer pool
  uint64_t max_bytes() const;

  // Return the approximate number of bytes this class consumes. This also includes the eviction queue.
  size_t memory_consumption() const;

  // Return the number of items that have been added to the eviction queue
  uint64_t num_eviction_queue_adds() const;

  // Return the number of items that have been purged from the eviction queue
  uint64_t num_eviction_queue_items_purged() const;

  // Return the total number of page evictions
  uint64_t num_evictions() const;

  // Return the total number of bytes that have been evicted to the disk
  uint64_t total_bytes_evicted_to_ssd() const;

  // Resize the buffer pool. This is not thread-safe and should only be called when no other threads are accessing the buffer pool.
  void resize(const uint64_t new_size);

 private:
  friend class BufferManager;
  friend class BufferPoolTest;

  // Item for the Eviction Queue
  struct EvictionItem final {
    // The page to be evicted.
    PageID page_id;

    // Insertion timestamp for frame into the queue. Is compared with eviction_timestamp of frame.
    uint64_t timestamp;

    // Check if the given frame can be evicted if it was marked before
    bool can_evict(Frame::StateVersionType state_and_version) const;

    // Check if the given frame can be marked for eviction
    bool can_mark(Frame::StateVersionType state_and_version) const;
  };

  using EvictionQueue = moodycamel::ConcurrentQueue<EvictionItem>;

  static constexpr size_t PURGE_INTERVAL = 1024;

  BufferPool(const size_t pool_size,
             const std::array<std::shared_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> volatile_regions,
             const std::shared_ptr<PersistenceManager> persistence_manager, const NodeID numa_node);

  // Reserve the given number of bytes. If there is not enough space, the eviction queue is used for eviction
  bool ensure_free_pages(const uint64_t bytes);

  // Purge the eviction queue to remove outdated items
  void purge_eviction_queue();

  // Add a page to the eviction queue
  void add_to_eviction_queue(const PageID page_id);

  // Evict the given item from the buffer pool
  void evict(EvictionItem& item, Frame* frame);

  // Reserve the given number of bytes. This increases the used bytes counter.
  uint64_t reserve_bytes(const uint64_t bytes);

  // Free the given number of bytes. This decreases the used bytes counter.
  void free_reserved_bytes(const uint64_t bytes);

  // The maximum number of bytes that can be allocated
  uint64_t _max_bytes;

  // The number of bytes that are currently used
  std::atomic_uint64_t _used_bytes;

  const std::shared_ptr<PersistenceManager> _persistence_manager;

  // Eviction queue for frames that are not pinned
  EvictionQueue _eviction_queue;

  const std::array<std::shared_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> _volatile_regions;

  const NodeID _node_id;

  std::atomic_uint64_t _num_eviction_queue_items_purged = 0;

  std::atomic_uint64_t _num_eviction_queue_adds = 0;

  std::atomic_uint64_t _num_evictions = 0;

  std::atomic_uint64_t _total_bytes_evicted_to_ssd = 0;
};
}  // namespace hyrise
