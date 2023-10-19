#pragma once

#include <atomic>
#include <boost/container/pmr/memory_resource.hpp>
#include <mutex>
#include <stack>
#include "buffer_manager.hpp"

namespace hyrise {

/**
 * The PageAllocator allocates pages on the buffer pool using the buffer manager.
 * Given a number of bytes, it rounds up the page size type to the next page size type e.g. 6 KiB result in the allocation of a 8KiB page.
*/
class PageAllocator : public boost::container::pmr::memory_resource {
 public:
  // Create a PageAllocator for a given buffer manager
  PageAllocator(BufferManager* _buffer_manager);

  // Allocate a number of bytes. Allocated memory is rounded up to the next page size type e.g. 6 KiB result in the allocation of a 8KiB page.
  void* do_allocate(std::size_t bytes, std::size_t alignment) override;

  // Deallocate a given page. The page must be unpinned.
  void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override;

  // Check if two PageAllocators are equal
  bool do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept override;

  // Get the number of allocations
  uint64_t allocation_count() const;

  // Get the number of deallocations
  uint64_t deallocation_count() const;

  // Get the total number of allocated bytes
  uint64_t allocated_bytes() const;

 private:
  // Get a new page id for a given size type
  PageID new_page_id(const PageSizeType size_type);

  // Free a page by adding it to the free page stack
  void free_page_id(const PageID page_id);

  BufferManager* _buffer_manager;

  // One mutex per page size type
  std::array<std::mutex, NUM_PAGE_SIZE_TYPES> _mutexes;

  // Number of pages per page size type
  std::array<uint64_t, NUM_PAGE_SIZE_TYPES> _num_pages;

  // One stack of free pages per page size type
  std::array<std::stack<PageID>, NUM_PAGE_SIZE_TYPES> _free_page_ids;

  // Tracks the number of allocations
  std::atomic_uint64_t _allocation_count;

  // Tracks the number of deallocations
  std::atomic_uint64_t _deallocation_count;

  // Tracks the number of allocated bytes
  std::atomic_uint64_t _allocated_bytes;
};
}  // namespace hyrise