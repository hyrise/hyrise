#pragma once

#include <boost/dynamic_bitset.hpp>
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

// TODO: Reduce usage of frames with LeakyBuffer from Spitfire

class VolatileRegion : public Noncopyable {
 public:
  VolatileRegion(const PageSizeType size_type, const PageType page_type, const size_t num_bytes,
                 const int8_t memory_numa_node = NO_NUMA_MEMORY_NODE);
  ~VolatileRegion();

  std::pair<FramePtr, std::ptrdiff_t> unswizzle(const void* ptr);

  void allocate(FramePtr frame);
  void move(FramePtr from, FramePtr to);
  void deallocate(FramePtr frame);
  void free(FramePtr frame);

  // Total number of bytes in the region
  size_t capacity() const;

  // Unmap and remap memory region and reassign memory to frames
  void clear();

 private:
  void unmap_memory();
  void map_memory();

  void to_numa(std::byte* address);

  const PageType _page_type;
  const size_t _numa_memory_node;
  const size_t _total_bytes;
  const PageSizeType _size_type;

  std::byte* _mapped_memory;

  std::vector<FramePtr> _frames;
  boost::dynamic_bitset<> _free_slots;

  std::mutex _mutex;
};

std::array<std::unique_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> create_volatile_regions_for_size_types(
    const PageType page_type, const size_t num_bytes);

}  // namespace hyrise