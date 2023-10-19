#pragma once

#include <atomic>
#include <boost/container/pmr/memory_resource.hpp>
#include <mutex>
#include <stack>
#include "buffer_manager.hpp"

namespace hyrise {

/**
 * The PageAllocator is a memory resource for the buffer manager. It allocates and deallocates pages from the buffer pool.
 * Given a number of bytes, it rounds up the page size type to the next page size type e.g. 6 KiB result in the allocation of a 8KiB page.
 * 
*/
class PageAllocator : public boost::container::pmr::memory_resource {
 public:
  // Create a PageAllocator for a given buffer manager
  PageAllocator(BufferManager* _buffer_manager);

  // Allocate a number of bytes. Allocated memory is rounded up to the next page size type e.g. 6 KiB result in the allocation of a 8KiB page.
  void* do_allocate(std::size_t bytes, std::size_t alignment) override;

  // Deallocate a given page. The page must be unpinned.
  void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override;

  bool do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept override;

  // Get the number of allocations
  uint64_t num_allocs() const;

  // Get the number of deallocations
  uint64_t num_deallocs() const;

  // Get the total number of allocated bytes
  uint64_t total_allocated_bytes() const;

 private:
  // Get a new page id for a given size type
  PageID new_page_id(const PageSizeType size_type);

  void free_page_id(const PageID page_id);

  BufferManager* _buffer_manager;

  std::array<std::mutex, NUM_PAGE_SIZE_TYPES> _mutexes;
  std::array<uint64_t, NUM_PAGE_SIZE_TYPES> _num_pages;

  std::array<std::stack<PageID>, NUM_PAGE_SIZE_TYPES> _free_page_ids;

  std::atomic_uint64_t _num_allocs;

  std::atomic_uint64_t _num_deallocs;

  std::atomic_uint64_t _total_allocated_bytes;
};
}  // namespace hyrise