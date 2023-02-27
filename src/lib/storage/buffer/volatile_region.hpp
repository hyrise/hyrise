#pragma once

#include <boost/align/aligned_allocator.hpp>
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
  VolatileRegion(const size_t num_bytes);

  FrameID get_frame_id_from_ptr(const void* ptr) const;
  Page32KiB* get_page(const FrameID frame_id);

  std::pair<FrameID, Page32KiB*> allocate();
  void deallocate(FrameID frame_id);

  size_t capacity() const;
  size_t size() const;

 private:
  // Total number of bytes allocated in data
  const size_t _num_bytes;

  // The raw memory region that is preallocated for the frames
  std::vector<Page32KiB, boost::alignment::aligned_allocator<Page32KiB>> _frames;

  Page32KiB* _free_list;

  size_t _num_free_frames;

  // std::mutex _mutex;
};
}  // namespace hyrise