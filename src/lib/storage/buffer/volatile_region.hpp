#pragma once

#include <forward_list>
#include <memory>
#include <mutex>
#include "frame.hpp"
#include "storage/buffer/types.hpp"

namespace hyrise {

/**
 * @brief Main-Memory pool consisting of chunks (= pages) of memory. A frame acts as a slot 
 * for pages. In order to allocate multiple, contiguous pages. The memory region keeps a sorted list
 * of free frames that can be popped.
 * 
 * TODO: Make concurrent e.g boost::lockfree::stack
 * TODO: Evaluate best-fit, first-fit etc
 * TODO: Pin Complete pool to not swap out e.g. mlock
 */

class VolatileRegion {
 public:
  VolatileRegion(const size_t num_bytes, const PageSizeType size_type);
  ~VolatileRegion();

  FrameID to_frame_id(const void* ptr) const;
  std::byte* get_page(const FrameID frame_id);

  std::pair<FrameID, std::byte*> allocate();
  void deallocate(FrameID frame_id);

  size_t capacity() const;
  size_t size() const;

  PageSizeType get_size_type() const;

 private:
  static std::byte* allocate_aligned_frames(const size_t num_frames, const PageSizeType size_type);

  // Total number of bytes allocated in data
  const size_t _num_frames;

  const PageSizeType _size_type;

  // The raw memory region that is preallocated for the frames
  std::byte* _frames;  // CONST

  // Linked List for free pages
  std::byte* _free_list;

  // Number of free frames to be used
  size_t _num_free_frames;
};

}  // namespace hyrise