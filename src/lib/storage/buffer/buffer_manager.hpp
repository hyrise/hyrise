#pragma once

#include <filesystem>
#include <memory>
#include <tuple>
#include "noncopyable.hpp"
#include "storage/buffer/buffer_pool.hpp"
#include "storage/buffer/frame.hpp"
#include "storage/buffer/persistence_manager.hpp"
#include "storage/buffer/volatile_region.hpp"

namespace hyrise {

uint64_t get_default_pool_size();

/**
 * The buffer manager is responsible for managing the working memory of Hyrise. The buffer manager can automatically offload pages to another NUMA node or to the SSD if 
 * the used amount of memory exceeds the sytem amount of memory. Pinning ensures that pages are not evicted from the buffer pool during an operation. Compared to umap, mmap etc.
 * this implementation is transaction-safe, thread-safe, extendable and allows fine-grained control over the memory management. In constrast to tiering, the buffer manager does not
 * require a placement algorithm and is therefore more flexible.
 * 
 * Instead of relying on traditional buffer manager techniques with a global hash map or pointer swizzling, we leverage anonymous virtual memory and manual page table manipulation.
 * This basic implementation was presented in the SIGMOD'23 paper "Virtual-Memory Assisted Buffer Management" by Leis et. al. We further support explicitly defined variable sized pages to handle differently sized segments. For this, 
 * we employ a Second-Chance FIFO for page replacement when a memory limit is reached.
 * 
 * 
 * Usage:
 * Usually, the global buffer manager is accessed via the Hyrise singleton to ensure proper creation and cleanup order. We provide several Guard primitives (defined in pin_guard.hpp) 
 * so simplify the usage of the buffer manager with vectors and segments in operators. For better memory allocation, we usually proxy allocations through an additional, stateless JemallocMemoryResource.
 *  * TODO: Pinning, concurrreny,  NUMA, SSD, metrics, debugging

 * Future Work: The low-level API presented here can be used to implement a variety of buffer management techniques. We see the following extensions to improve the performance:
 * - Use Async I/O for reading and writing pages to the SSD with libaio or io_uring
 * - Improve NUMA-awareness and support more complex topologies; currently only one NUMA node for execution and one memory node are supported
 * - Rethink handling of variable-sized pages
 * - Implemented custom data structures with the buffer manager in mind
 * - MVCC, Recovery using WAL
 * - Optimistic Latching without using the locking mechanism
*/
class BufferManager final : public Noncopyable {
 public:
  // Create a buffer manager with the given pool size and path to the SSD storage
  explicit BufferManager(const uint64_t pool_size = get_default_pool_size(),
                         const std::filesystem::path ssd_path = std::filesystem::current_path() / "buffer_manager_data",
                         const NodeID node_id = NodeID{0});

  // Destructor which cleanups all resources
  ~BufferManager();

  // Move assignment operator. The operation is not thread-safe.
  BufferManager& operator=(BufferManager&& other) noexcept;

  // Pin a page exclusively in the buffer pool for single writers. This ensures that the page is not evicted from the buffer pool.
  void pin_exclusive(const PageID page_id);

  // Pin a page in shared mode in the buffer pool for multiple readers. This ensures that the page is not evicted from the buffer pool.
  void pin_shared(const PageID page_id);

  // Unpin a page from the buffer pool. This allows the page to be evicted from the buffer pool.
  void unpin_exclusive(const PageID page_id);

  // Unpin a page from the buffer pool. This allows the page to be evicted from the buffer pool.
  void unpin_shared(const PageID page_id);

  // Mark a page as dirty
  void set_dirty(const PageID page_id);

  // Find the page id for a given virtual memory address. The PageID can be invalid if the address is not part of the buffer pool.
  PageID find_page(const void* ptr) const;

  // Helper method for return the state of a page in the buffer manager
  Frame::StateVersionType page_state(const PageID page_id);

  // Get an approximation of the memory consumption of the buffer manager in bytes. This does not include the memory consumption of the buffer pools.
  size_t memory_consumption() const;

  // Get the buffer pools of the buffer manager. The result also provides the memory consumption of each buffer pool.
  const BufferPool& buffer_pool() const;

  // Get the persistence manager of the buffer manager. The persistence manager is responsible for reading and writing pages to the disk.
  const PersistenceManager& persistence_manager() const;

  // Get the total number of hits in the buffer manager
  uint64_t total_hits() const;

  // Get the total number of misses in the buffer manager
  uint64_t total_misses() const;

  // Get the total number of pins in the buffer manager
  uint64_t total_pins() const;

 private:
  friend class Hyrise;
  friend class PageMemoryResource;

  // Get the region for the given page id
  std::shared_ptr<VolatileRegion> _get_region(const PageID page_id);

  // Protect a page from access
  void _protect_page(const PageID page_id);

  // Unprotect a page from access
  void _unprotect_page(const PageID page_id);

  // Load a page into the main memory
  void _make_resident(const PageID page_id, const Frame::StateVersionType previous_state_version);

  // The complete memory region of the buffer manager
  std::byte* _mapped_region;

  // A fixed size array of volatile regions for each page size type
  std::array<std::shared_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> _volatile_regions;

  // The persistence manager is responsible for reading and writing pages to the disk
  std::shared_ptr<PersistenceManager> _persistence_manager;

  // The buffer pool is responsible for managing the memory of the buffer manager
  BufferPool _buffer_pool;

  // The total number of hits in the buffer manager
  std::atomic_uint64_t _total_hits;

  // The total number of misses in the buffer manager
  std::atomic_uint64_t _total_misses;

  // The total number of pins in the buffer manager
  std::atomic_uint64_t _total_pins;
};

}  // namespace hyrise