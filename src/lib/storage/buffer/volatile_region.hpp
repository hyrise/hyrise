#pragma once

#include <atomic>
#include <boost/dynamic_bitset.hpp>
#include <forward_list>
#include <memory>
#include <mutex>
#include <span>
#include <tuple>
#include "frame.hpp"
#include "page_id.hpp"
#include "types.hpp"

/**
 * @brief The VolatileRegion class provides methods for page and frame access and NUMA page movement for a defined page size type. 
 * 
 * The class uses MADV_FREE_REUSABLE and MADV_DONTNEED on Linux to free pages. On OS X, it uses MADV_FREE_REUSABLE to free pages and MADV_FREE_REUSE to mark pages as reusable. The class provides two methods for freeing pages: free() and reuse(). The free() method uses MADV_FREE_REUSABLE on OS X and MADV_DONTNEED on Linux to free a page. The reuse() method uses MADV_FREE_REUSE on OS X to mark a page as reusable. This method is not implemented on Linux.
 * 
 * The class provides three methods for moving pages between NUMA nodes: mbind_to_numa_node(), move_page_to_numa_node(), and memcpy_page_to_numa_node(). The mbind_to_numa_node() method uses mbind to move a page to a NUMA node. This method is not portable and only works on Linux with NUMA support. The move_page_to_numa_node() method uses move_pages to move a page to a NUMA node. This method is not portable and only works on Linux with NUMA support. The memcpy_page_to_numa_node() method uses double memcpy + madvise(MADV_DONTNEED) with an intermediate buffer to move a page to a NUMA node. This method is faster with more concurrency.
 */
namespace hyrise {
class VolatileRegion final : public Noncopyable {
 public:
  // Default size of the virtual memory region for all page size types. Default is 256 GB.
  constexpr static uint64_t DEFAULT_RESERVED_VIRTUAL_MEMORY = 1UL << 38;
  constexpr static uint64_t DEFAULT_RESERVED_VIRTUAL_MEMORY_PER_REGION =
      (DEFAULT_RESERVED_VIRTUAL_MEMORY / PAGE_SIZE_TYPES_COUNT) / bytes_for_size_type(MAX_PAGE_SIZE_TYPE) *
      bytes_for_size_type(MAX_PAGE_SIZE_TYPE);

  using MappedRegion = std::span<std::byte, DEFAULT_RESERVED_VIRTUAL_MEMORY>;
  using MappedPageRegion = std::span<std::byte, DEFAULT_RESERVED_VIRTUAL_MEMORY_PER_REGION>;

  // Enable mprotect class for debugging purposes
  constexpr static bool ENABLE_MPROTECT = false;

  // Create a VolatileRegion in a memory region for a given size_type.
  VolatileRegion(const PageSizeType size_type, MappedPageRegion region);

  // Get the frame of a given page.
  Frame* get_frame(const PageID page_id);

  // Get the start address of a given page.
  std::byte* get_page(const PageID page_id);

  // Use mbind for page movement. This is not portable and only works on Linux with NUMA support.
  void mbind_to_numa_node(const PageID page_id, const NodeID target_memory_node);

  // Use move_pages for page movement. This is not portable and only works on Linux with NUMA support.
  void move_page_to_numa_node(const PageID page_id, const NodeID target_memory_node);

  // Use double memcpy + madvise(MADV_DONTNEED) with intermediate buffer for page movement. This is faster with more concurrency.
  void memcpy_page_to_numa_node(const PageID page_id, const NodeID target_memory_node);

  // Free a page using madvise(MAV_FREE_REUSABLE) on OS X and madvise(MADV_DONTNEED) on Linux.
  void free(const PageID page_id);

  // Mark a page as reusable using madvise(MADV_FREE_REUSE) on OS X to update memory accounting. Not implemented on Linux.
  void reuse(const PageID page_id);

  // Returns the number of pages this region can manage.
  size_t size() const;

  // Returns the size type of this region.
  PageSizeType size_type() const;

  // Calculate the approximate memory used by this object.
  size_t memory_consumption() const;

  // Create a fixed-sized memory region using mmap that can be divided into regions of pages for all size types.
  static MappedRegion create_mapped_region();

  // Unmap the fixed-sized memory region
  static void unmap_region(MappedRegion region);

  // Create a VolatileRegion for each size type from the fixed-sized memory region.
  static std::array<std::shared_ptr<VolatileRegion>, PAGE_SIZE_TYPES_COUNT> create_volatile_regions(
      MappedRegion mapped_region);

  // Returns the number of madvice(MADV_FREE_REUSABLE) calls on OS X or madvice(MADV_DONTNEED) calls on Linux.
  uint64_t madvice_free_call_count() const;

  // Returns the number of page moevements using mbind or move_pages.
  uint64_t numa_page_movement_count() const;

 private:
  // Calls mprotect on a given page. Subsequent accesses to the page produce a segfault. Used for debugging.
  void _protect_page(const PageID page_id);

  // Calls mprotect on a given page to unprotect it. Used for debugging.
  void _unprotect_page(const PageID page_id);

  // Number of madvice(MADV_FREE_REUSABLE) calls on OS X or madvice(MADV_DONTNEED) calls on Linux
  std::atomic_uint64_t _madvice_free_call_count = 0;

  // Number of page moevements using mbind or move_pages
  std::atomic_uint64_t _numa_page_movement_count = 0;

  const PageSizeType _size_type;

  MappedPageRegion _region;

  std::vector<Frame> _frames;
};

}  // namespace hyrise
