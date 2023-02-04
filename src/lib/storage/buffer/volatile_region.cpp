#include "volatile_region.hpp"

namespace hyrise {
VolatileRegion::VolatileRegion(size_t num_bytes) : _num_bytes(num_bytes) {
  _data = std::make_unique<std::byte[]>(num_bytes);
  Assert(_data.get() != nullptr, "Could not properly allocate data for volatile region.");
  for (FrameID frame_id{0}; frame_id < capacity(); frame_id++) {
    _free_frames.push_front(frame_id);
  }
  _num_free_frames = capacity();
  // TODO: This could conflict with aligner, maybe store the Frame somewhere else
  Assert(_num_free_frames > 0, "There should be at least one free frame in the volatile region while setting up.");
}

Frame* VolatileRegion::allocate() {
  if (_num_free_frames <= 0) {
    return nullptr;
  }
  const auto frame_id = _free_frames.front();
  _free_frames.pop_front();
  _num_free_frames--;
  return reinterpret_cast<Frame*>(_data.get() + frame_id * sizeof(Frame));
};

FrameID VolatileRegion::get_frame_id_from_ptr(const void* ptr) const {
  DebugAssert(_data.get() <= ptr, "Pointer is out of range of region");
  DebugAssert(ptr < _data.get() + capacity() * sizeof(Frame), "Pointer is out of range of region");
  const auto offset = reinterpret_cast<const std::byte*>(ptr) - _data.get();
  return FrameID{offset / sizeof(Frame)};
}

Frame* VolatileRegion::get_frame_from_frame_id(const FrameID frame_id) const {
  DebugAssert(frame_id < capacity(), "Cannot request a frame id larger than capacity.");
  return reinterpret_cast<Frame*>(_data.get() + frame_id * sizeof(Frame));
}

Frame* VolatileRegion::get_frame_from_ptr(const void* ptr) const {
  DebugAssert(_data.get() <= ptr, "Pointer is out of range of region");
  DebugAssert(ptr < _data.get() + capacity() * sizeof(Frame), "Pointer is out of range of region");
  const auto frame_id = get_frame_id_from_ptr(ptr);
  return get_frame_from_frame_id(frame_id);
}

void VolatileRegion::deallocate(Frame* frame) {
  Assert(_data.get() <= reinterpret_cast<std::byte*>(frame),
         "Deallocated frame has to be in the volatile memory region");
  Assert(reinterpret_cast<std::byte*>(frame) <= _data.get() + _num_bytes,
         "Dellocated frame has to be in the volatile memory region");  // TODO: Might not be the exact boundary
  const auto frame_id = static_cast<FrameID>((reinterpret_cast<std::byte*>(frame) - _data.get()) / sizeof(Frame));
  _free_frames.push_front(frame_id);
  _num_free_frames++;
};

size_t VolatileRegion::capacity() const {
  return _num_bytes / sizeof(Frame);
}

size_t VolatileRegion::size() const {
  return capacity() - _num_free_frames;
}

}  // namespace hyrise
