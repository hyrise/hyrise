#pragma once

#include <memory>
#include <utility>
#include "storage/buffer/clock_replacement_strategy.hpp"
#include "storage/buffer/frame.hpp"
#include "storage/buffer/ssd_region.hpp"
#include "storage/buffer/types.hpp"
#include "storage/buffer/volatile_region.hpp"

namespace hyrise {

template <typename PointedType>
class BufferManagedPtr;

/**
 * @brief 
 * 
 * TODO: Ensure concurrent access via atomics
 */
class BufferManager {
 public:
  struct Metrics {
    // Tracks all allocation that are happening on the buffer manager through the BufferPoolAllocator
    std::vector<std::size_t> allocations_in_bytes{};

    // The maximum amount of bytes being allocated with with subsequent calls of alloc and dealloc
    std::size_t max_bytes_used;

    // The current amount of bytes being allocated
    std::size_t current_bytes_used;

    // The total number of bytes being allocates
    std::size_t total_allocated_bytes;

    // The number of allocation
    std::size_t num_allocs;

    // Tracks the number of hits in the page_table
    std::size_t page_table_hits = 0;

    // Tracks the number of hits in the page_table
    std::size_t page_table_misses = 0;

    // Tracks the number of frames in the volatile region
    std::size_t num_frames = 0;

    // Tracks the number of bytes written to SSD
    std::size_t bytes_written = 0;

    // Tracks the number of bytes read from SSD
    std::size_t bytes_read = 0;

    // TODO: Number of pages used, fragmentation rate
  };

  BufferManager(std::unique_ptr<VolatileRegion> volatile_region, std::unique_ptr<SSDRegion> ssd_region);

  /**
   * @brief Get the page object
   * 
   * @param page_id 
   * @return std::unique_ptr<Page> 
   */
  Page32KiB* get_page(const PageID page_id);

  /**
   * @brief 
   * 
   * @return std::pair<PageID, std::unique_ptr<Page>> 
   */
  PageID new_page();

  /**
   * @brief Pin a page marks a page unavailable for replacement. It needs to be unpinned before it can be replaced.
   * 
   * @param page_id 
   */
  void pin_page(const PageID page_id);

  /**
   * @brief Unpinning a page marks a page available for replacement. This acts as a soft-release without flushing
   * the page back to disk. Calls callback if the pin count is redced to zero.
   * 
   * @param page_id 
   */
  void unpin_page(const PageID page_id);

  void mark_page_dirty(const PageID page_id);

  /**
   * @brief 
   * 
   * @param page_id 
   */
  void flush_page(const PageID page_id);

  /**
   * @brief Remove a page completely from the buffer manager and any of the storage regions
   * 
   * @param page_id 
   */
  void remove_page(const PageID page_id);

  /**
   * @brief Get the page id and offset from ptr object. PageID is on its max 
   * if the page there was no page found
   * 
   * @param ptr 
   * @return std::pair<PageID, PageOffset> 
   */
  std::pair<PageID, std::ptrdiff_t> get_page_id_and_offset_from_ptr(const void* ptr);

  /**
   * Allocates pages to fullfil allocation request of the given bytes and alignment 
  */
  BufferManagedPtr<void> allocate(std::size_t bytes, std::size_t align = alignof(std::max_align_t));

  /**
   * Deallocates a pointer and frees the pages.
  */
  void deallocate(BufferManagedPtr<void> ptr, std::size_t bytes, std::size_t align = alignof(std::max_align_t));

  /**
   * @brief Helper function to get the BufferManager singleton. This avoids issues with circular dependencies as the implementation in the .cpp file.
   * 
   * @return BufferManager& 
   */
  static BufferManager& get_global_buffer_manager();

  /**
   * @brief Returns a metrics struture holding information about allocations, page table hits etc. of the current buffer manager instance. Can be reset by
   * assigning a new instance
  */
  Metrics& metrics();

 protected:
  friend class Hyrise;

 private:
  BufferManager();

  std::pair<FrameID, Frame*> allocate_frame();

  Frame* find_in_page_table(const PageID page_id);

  void read_page(const PageID page_id, Page32KiB& destination);
  void write_page(const PageID page_id, Page32KiB& source);

  size_t _num_pages;

  // Memory Region for pages on SSD
  std::unique_ptr<SSDRegion> _ssd_region;

  // Memory Region for the main memory aka buffer pool
  std::unique_ptr<VolatileRegion> _volatile_region;

  // Page Table that contains frames (= pages) which are currently in the buffer pool
  std::unordered_map<PageID, Frame*> _page_table;

  // Metadata storage of frames
  std::vector<Frame> _frames;

  // The replacement strategy used then the page table is full and the to be evicted frame needs to be selected
  std::unique_ptr<ClockReplacementStrategy> _replacement_strategy;

  // Metrics of buffer manager
  Metrics _metrics{};
};

}  // namespace hyrise