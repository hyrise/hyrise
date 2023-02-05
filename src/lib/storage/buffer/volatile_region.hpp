#pragma once

#include <forward_list>
#include <memory>
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
  VolatileRegion(size_t num_bytes);

  // TODO: Inline
  FrameID get_frame_id_from_ptr(const void* ptr) const;
  Frame* get_frame_from_ptr(const void* ptr) const;
  Frame* get_frame_from_frame_id(const FrameID frame_id) const;

  Frame* allocate();
  void deallocate(Frame* frame);

  size_t capacity() const;
  size_t size() const;

 private:
  // Total number of bytes allocated in data
  const size_t _num_bytes;

  // The raw memory region that is preallocated for the frames
  std::unique_ptr<std::byte[]> _data;

  // TODO: Evalulate linked list vs other DS, or maybe sort to fullfil large requests?
  // it might also be concurrent
  std::forward_list<FrameID> _free_frames;
  size_t _num_free_frames;
};
}  // namespace hyrise