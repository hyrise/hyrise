#include "volatile_region.hpp"

namespace hyrise {
VolatileRegion::VolatileRegion(const size_t num_bytes) : _num_bytes(num_bytes) {
  _data = std::make_unique<std::byte[]>(num_bytes);  // TODO: Do alinged_alloc
  Assert(_data.get() != nullptr, "Could not properly allocate data for volatile region.");
  for (FrameID frame_id{0}; frame_id < capacity(); frame_id++) {
    _free_frames.push_front(frame_id);
  }
  _num_free_frames = capacity();
  // TODO: This could conflict with aligner, maybe store the Frame somewhere else
  Assert(_num_free_frames > 0, "There should be at least one free frame in the volatile region while setting up.");
}

std::pair<FrameID, Page*> VolatileRegion::allocate() {
  if (_num_free_frames <= 0) {
    return std::make_pair(INVALID_FRAME_ID, nullptr);
  }
  const auto frame_id = _free_frames.front();
  _free_frames.pop_front();
  _num_free_frames--;
  const auto page = reinterpret_cast<Page*>(_data.get() + frame_id * sizeof(Page));
  return std::make_pair(frame_id, page);
};

FrameID VolatileRegion::get_frame_id_from_ptr(const void* ptr) const {
  DebugAssert(_data.get() <= ptr, "Pointer is out of range of region");
  DebugAssert(ptr < (_data.get() + capacity() * sizeof(Page)), "Pointer is out of range of region");
  const auto offset = reinterpret_cast<const std::byte*>(ptr) - _data.get();
  return FrameID{offset / sizeof(Page)};
}

Page* VolatileRegion::get_page(const FrameID frame_id) const {
  DebugAssert(frame_id < capacity(), "Cannot request a frame id larger than capacity.");
  return reinterpret_cast<Page*>(_data.get() + frame_id * sizeof(Page));
}

void VolatileRegion::deallocate(FrameID frame_id) {
  DebugAssert(frame_id < capacity(), "Cannot request a frame id larger than capacity.");
  // const auto frame_id = static_cast<FrameID>((reinterpret_cast<std::byte*>(frame) - _data.get()) / sizeof(Frame));
  _free_frames.push_front(frame_id);
  _num_free_frames++;
};

size_t VolatileRegion::capacity() const {
  return _num_bytes / sizeof(Page);
}

size_t VolatileRegion::size() const {
  return capacity() - _num_free_frames;
}

}  // namespace hyrise
