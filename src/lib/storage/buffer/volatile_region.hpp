#pragma once

#include <forward_list>
#include <memory>
#include "frame.hpp"
#include "types.hpp"

namespace hyrise {
/**
 * @brief 
 * 
 * TODO: Make concurrent e.g boost::lockfree::stack
 */
class VolatileRegion : private Noncopyable {
 public:
  VolatileRegion(size_t num_bytes);

  Frame* get(FrameID frame_id);

  Frame* allocate();
  void deallocate(Frame* frame);

  size_t capacity() const;
  size_t size() const;

 private:
  // Total number of bytes allocated in data
  const size_t _num_bytes;

  // The raw memory region that is preallocated for the frames
  std::unique_ptr<std::byte[]> _data;

  // TODO: Evalulate linked list vs other DS
  std::forward_list<FrameID> _free_frames;
  size_t _num_free_frames;
};
}  // namespace hyrise