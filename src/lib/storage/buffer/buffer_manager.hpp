#pragma once
#include <memory>
#include <utility>
#include "storage/buffer/clock_replacement_strategy.hpp"
#include "storage/buffer/frame.hpp"
#include "storage/buffer/ssd_region.hpp"
#include "storage/buffer/volatile_region.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * @brief 
 * 
 * TODO: Ensure concurrent access via latches or other lock-free mechanims
 */
class BufferManager : public Noncopyable {
 public:
  BufferManager(std::unique_ptr<VolatileRegion> volatile_region, std::unique_ptr<SSDRegion> ssd_region);

  /**
   * @brief Get the page object
   * 
   * @param page_id 
   * @return std::unique_ptr<Page> 
   */
  std::unique_ptr<Page> get_page(const PageID page_id);

  /**
   * @brief 
   * 
   * @return std::pair<PageID, std::unique_ptr<Page>> 
   */
  std::pair<PageID, std::unique_ptr<Page>> new_page();

  /**
   * @brief Pin a page marks a page unavailable for replacement. It needs to be unpinned before it can be replaced.
   * 
   * @param page_id 
   */
  void pin_page(const PageID page_id);

  /**
   * @brief Unpinning a page marks a page available for replacement. This acts as a soft-release without flushing
   * the page back to disk.
   * 
   * @param page_id 
   */
  void unpin_page(const PageID page_id);

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

 protected:
  friend class Hyrise;

 private:
  BufferManager();

  size_t _num_pages;

  // Memory Region for pages on SSD
  std::unique_ptr<SSDRegion> _ssd_region;

  // Memory Region for the main memory aka buffer pool
  std::unique_ptr<VolatileRegion> _volatile_region;

  // Page Table that contains frames (= pages) which are currently in the buffer pool
  std::unordered_map<PageID, Frame*> _page_table;

  // The replacement strategy used then the page table is full and the to be evicted frame needs to be selected
  std::unique_ptr<ClockReplacementStrategy> _clock_replacement_strategy;
};
}  // namespace hyrise