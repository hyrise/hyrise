#pragma once

#include <boost/dynamic_bitset.hpp>
#include <boost/noncopyable.hpp>
#include <forward_list>
#include <memory>
#include <mutex>
#include "frame.hpp"
#include "storage/buffer/types.hpp"

namespace hyrise {

/**
 * @brief Main-Memory pool consisting of chunks (= pages) of memory. A frame acts as a slot 
 * for pages. In order to allocate multiple, contiguous pages. The memory region keeps a sorted list
 * of free frames that can be popped. The idea of reducing external fragmentation using MADVISE is taken from Umbra.
 */

class VolatileRegion : private boost::noncopyable {
 public:
  VolatileRegion(const PageSizeType size_type, const size_t num_bytes,
                 const size_t memory_numa_node = NO_NUMA_MEMORY_NODE);
  ~VolatileRegion();

  Frame* unswizzle(const void* ptr);

  Frame* allocate();
  void deallocate(Frame* frame);
  void free(Frame* frame);

  // Total number of bytes in the region
  size_t capacity() const;

  // Unmap and remap memory region and reassign memory to frames
  void clear();

 private:
  void create_free_list();
  void assign_memory_to_frames();
  void unmap_memory();
  void map_memory();

  void to_numa(std::byte* address);

  const size_t _memory_numa_node;
  const size_t _total_bytes;
  const PageSizeType _size_type;

  std::byte* _mapped_memory;

  Frame* _free_list;

  std::vector<Frame> _frames;

  std::mutex _mutex;
};

}  // namespace hyrise