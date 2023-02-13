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

std::pair<FrameID, Page32KiB*> VolatileRegion::allocate() {
  std::lock_guard<std::mutex> lock_guard(_mutex);

  if (_num_free_frames <= 0) {
    return std::make_pair(INVALID_FRAME_ID, nullptr);
  }
  const auto frame_id = _free_frames.front();
  _free_frames.pop_front();
  _num_free_frames--;
  const auto page = reinterpret_cast<Page32KiB*>(_data.get() + frame_id * Page32KiB::size());
  return std::make_pair(frame_id, page);
};

FrameID VolatileRegion::get_frame_id_from_ptr(const void* ptr) const {
  DebugAssert(_data.get() <= ptr, "Pointer is out of range of region");
  DebugAssert(ptr < (_data.get() + capacity() * Page32KiB::size()), "Pointer is out of range of region");
  const auto offset = reinterpret_cast<const std::byte*>(ptr) - _data.get();
  return FrameID{offset / Page32KiB::size()};
}

Page32KiB* VolatileRegion::get_page(const FrameID frame_id) const {
  DebugAssert(frame_id < capacity(), "Cannot request a frame id larger than capacity.");
  return reinterpret_cast<Page32KiB*>(_data.get() + frame_id * Page32KiB::size());
}

void VolatileRegion::deallocate(FrameID frame_id) {
  std::lock_guard<std::mutex> lock_guard(_mutex);
  DebugAssert(frame_id < capacity(), "Cannot request a frame id larger than capacity.");
  // const auto frame_id = static_cast<FrameID>((reinterpret_cast<std::byte*>(frame) - _data.get()) / sizeof(Frame));
  _free_frames.push_front(frame_id);
  _num_free_frames++;
};

size_t VolatileRegion::capacity() const {
  return _num_bytes / Page32KiB::size();
}

size_t VolatileRegion::size() const {
  return capacity() - _num_free_frames;
}

}  // namespace hyrise
