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

  VolatileRegion(const PageSizeType size_type, const PageType page_type, const size_t num_bytes,
                 std::byte* premapped_region);
  Frame* frame_at_index(const size_t index) const;

  void allocate(FramePtr frame);
  void move(FramePtr from, FramePtr to);
  void deallocate(FramePtr frame);
  void free(FramePtr frame);

  std::byte* mapped_memory() const;
  size_t useable_bytes() const;
  size_t total_bytes() const;

  PageType get_page_type() const;
  PageSizeType get_size_type() const;

  void clear();

  size_t memory_consumption() const;

  static std::byte* map_memory(const size_t num_bytes, const int8_t numa_memory_node = NO_NUMA_MEMORY_NODE);
  static void unmap_memory(std::byte* memory, const size_t num_bytes);

 private:
  const PageType _page_type;
  const PageSizeType _size_type;

  const size_t _total_bytes;
  const size_t _useable_bytes;

  std::byte* _mapped_memory;

  std::vector<Frame*> _frames;
  boost::dynamic_bitset<> _free_slots;

  std::mutex _mutex;
};

std::array<std::unique_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> create_volatile_regions_for_size_types(
    const PageType page_type, const size_t num_byte, const int8_t numa_memory_node = NO_NUMA_MEMORY_NODE);

}  // namespace hyrise