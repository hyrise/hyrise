#pragma once

#include <boost/dynamic_bitset.hpp>
#include <forward_list>
#include <memory>
#include <mutex>
#include "frame.hpp"
#include "noncopyable.hpp"
#include "storage/buffer/types.hpp"

namespace hyrise {

/**
 * @brief Main-Memory pool consisting of chunks (= pages) of memory. A frame acts as a slot 
 * for pages. In order to allocate multiple, contiguous pages. The memory region keeps a sorted list
 * of free frames that can be popped. The idea of reducing external fragmentation using MADVISE is taken from Umbra.
 */
class VolatileRegion : public Noncopyable {
 public:
  VolatileRegion(const PageSizeType size_type, std::byte* region_start, std::byte* region_end);

  Frame* get_frame(PageID page_id);
  std::byte* get_page(PageID page_id);

  std::byte* allocate(const NumaMemoryNode memory_node = DEFAULT_DRAM_NUMA_NODE);
  void deallocate(PageID page_id);

  void move_to_numa_node(PageID page_id, const NumaMemoryNode target_memory_node = DEFAULT_DRAM_NUMA_NODE);
  void free(PageID page_id);

  void clear();

  size_t memory_consumption() const;

 private:
  const PageSizeType _size_type;

  std::byte* _region_start;
  std::byte* _region_end;

  std::vector<Frame> _frames;
  boost::dynamic_bitset<> _free_slots;

  std::mutex _mutex;
};

}  // namespace hyrise