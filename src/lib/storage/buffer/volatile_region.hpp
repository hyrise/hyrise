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

  constexpr static uint64_t DEFAULT_RESERVED_VIRTUAL_MEMORY = 1UL << 38;  // 256 GiB

  // Create a VolatileRegion in a virtual memory region for a givem size_type. The approximate_size_bytes defines a initial number of frames to be created.
  VolatileRegion(const PageSizeType size_type, std::byte* region_start, std::byte* region_end);

  // Get the frame of a given page
  Frame* get_frame(PageID page_id);

  // Get the start address of a given page
  std::byte* get_page(PageID page_id);

  // Use mbind for page movement
  void mbind_to_numa_node(PageID page_id, const NodeID target_memory_node);

  // Use move_pages for page movement
  void move_page_to_numa_node(PageID page_id, const NodeID target_memory_node);

  // Free a page using madvise
  void free(PageID page_id);

  // Returns the number of pages this region can manage
  size_t size() const;

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

 private:
  std::atomic_uint64_t num_madvice_free_calls = 0;
  std::atomic_uint64_t num_numa_page_movements = 0;

  const PageSizeType _size_type;

  std::byte* _region_start;
  std::byte* _region_end;

  std::vector<Frame> _frames;
};

}  // namespace hyrise
