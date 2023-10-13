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
 * based on the migration policy. Most functions including the constructor are private to the buffer manager class. The exposed public functions can be used to gather statistics and metadata.
 * 
 * The implementation is inspired by DuckDB (https://github.com/duckdb/duckdb) and Kuzu (https://github.com/kuzudb/kuzu).
*/
class BufferPool final {
 public:
  // Return the approximate number of bytes this class consumes. This also includes the eviction queue.
  size_t memory_consumption() const;

  uint64_t get_num_eviction_queue_adds() const;

  uint64_t get_reserved_bytes() const;

  uint64_t get_max_bytes() const;

  NodeID get_node_id() const;

  uint64_t get_num_eviction_queue_items_purged() const;

  uint64_t get_num_evictions() const;

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

  bool ensure_free_pages(const PageSizeType size);

  void purge_eviction_queue();

  void add_to_eviction_queue(const PageID page_id, Frame* frame);

  void evict(EvictionItem& item, Frame* frame);

  uint64_t reserve_bytes(const uint64_t bytes);

  void free_reserved_bytes(const uint64_t bytes);

  // The maximum number of bytes that can be allocated
  uint64_t max_bytes;

  // The number of bytes that are currently used
  std::atomic_uint64_t used_bytes;

  const std::shared_ptr<PersistenceManager> persistence_manager;

  // Eviction queue for frames that are not pinned
  EvictionQueue eviction_queue;

  const std::array<std::shared_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> volatile_regions;

  const NodeID node_id;

  std::atomic_uint64_t num_eviction_queue_items_purged = 0;

  std::atomic_uint64_t num_eviction_queue_adds = 0;

  std::atomic_uint64_t num_evictions = 0;
};
}  // namespace hyrise