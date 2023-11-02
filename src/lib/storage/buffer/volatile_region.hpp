#pragma once

#include <atomic>
#include <boost/dynamic_bitset.hpp>
#include <forward_list>
#include <memory>
#include <mutex>
#include <tuple>
#include "frame.hpp"
#include "page_id.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * @brief Main-memory pool that manages pages of a specific types and their frames. 
 * 
 * The class provides methods for allocating, deallocating, NUMA page movement and portable calls for madvise. On OS X,
 * we use MADV_FREE_REUSABLE and MADV_DONTNEED on Linux. The class also provides helper methods for debugging memory accesses.
 */
class VolatileRegion final : public Noncopyable {
 public:
  // Enable mprotect class for debugging purposes
  constexpr static bool ENABLE_MPROTECT = false;

  // Default size of the virtual memory region for all page size types
  constexpr static uint64_t DEFAULT_RESERVED_VIRTUAL_MEMORY = 1UL << 38;  // 256 GiB

  // Create a VolatileRegion in a memory region for a givem size_type.
  VolatileRegion(const PageSizeType size_type, std::byte* region_start, std::byte* region_end);

  // Get the frame of a given page
  Frame* get_frame(PageID page_id);

  // Get the start address of a given page
  std::byte* get_page(PageID page_id);

  // Use mbind for page movement. This is not portable and only works on Linux with NUMA support.
  void mbind_to_numa_node(PageID page_id, const NodeID target_memory_node);

  // Use move_pages for page movement. This is not portable and only works on Linux with NUMA support.
  void move_page_to_numa_node(PageID page_id, const NodeID target_memory_node);

  // Free a page using madvise(MAV_FREE_REUSABLE) on OS X and madvise(MADV_DONTNEED) on Linux
  void free(PageID page_id);

  // Mark a page as reusable using madvise(MADV_FREE_REUSE) on OS X to update memory accounting. Not implemented on Linux.
  void reuse(PageID page_id);

  // Returns the number of pages this region can manage
  size_t size() const;

  // Returns the size type of this region
  PageSizeType size_type() const;

  // Calculate the approximate memory used by this object
  size_t memory_consumption() const;

  // Calls mprotect on a given page. Subsequent accesses to the page produce a segfault. Used for debugging.
  void _protect_page(const PageID page_id);

  // Calls mprotect on a given page to unprotect it. Used for debugging.
  void _unprotect_page(const PageID page_id);

  // Create a fixed-sized memory region using mmap that can be divided into regions of pages for all size types
  static std::byte* create_mapped_region();

  // Unmap the fixed-sized memory region
  static void unmap_region(std::byte* region);

  // Create a VolatileRegion for each size type from the fixed-sized memory region
  static std::array<std::shared_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> create_volatile_regions(
      std::byte* mapped_region);

  // Returns the number of madvice(MADV_FREE_REUSABLE) calls on OS X or madvice(MADV_DONTNEED) calls on Linux
  uint64_t madvice_free_call_count() const;

  // Returns the number of page moevements using mbind or move_pages
  uint64_t numa_page_movement_count() const;

 private:
  // Number of madvice(MADV_FREE_REUSABLE) calls on OS X or madvice(MADV_DONTNEED) calls on Linux
  std::atomic_uint64_t _madvice_free_call_count = 0;

  // Number of page moevements using mbind or move_pages
  std::atomic_uint64_t _numa_page_movement_count = 0;

  const PageSizeType _size_type;

  std::byte* _region_start;
  std::byte* _region_end;

  std::vector<Frame> _frames;
};

}  // namespace hyrise
