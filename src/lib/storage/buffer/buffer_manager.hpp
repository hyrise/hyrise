#pragma once
#include <memory>
#include "storage/buffer/clock_replacement_strategy.hpp"
#include "storage/buffer/frame.hpp"
#include "storage/buffer/ssd_region.hpp"
#include "storage/buffer/volatile_region.hpp"

#include "types.hpp"

namespace hyrise {

class BufferManager : public Noncopyable {
 public:
  BufferManager(std::unique_ptr<VolatileRegion> volatile_region, std::unique_ptr<SSDRegion> ssd_region);

  std::unique_ptr<Page> get_page(const PageID page_id);

  std::unique_ptr<Page> new_page();

  void flush_page(const PageID page_id);

 protected:
  friend class Hyrise;

 private:
  BufferManager();

  // Memory Region for pages on SSD
  std::unique_ptr<SSDRegion> _ssd_region;

  // Memory Region for the main memory aka buffer pool
  std::unique_ptr<VolatileRegion> _volatile_region;

  // Page Table that contains frames (= pages) which are currently in the buffer pool
  std::unordered_map<PageID, Frame> _page_table;

  // The replacement strategy used then the page table is full and the to be evicted frame needs to be selected
  std::unique_ptr<ClockReplacementStrategy> _clock_replacement_strategy;
};
}  // namespace hyrise