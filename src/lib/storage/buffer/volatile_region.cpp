#include "volatile_region.hpp"

namespace hyrise {

VolatileRegion::VolatileRegion(const size_t num_bytes) : _num_bytes(num_bytes), _frames(num_bytes / sizeof(Page32KiB)) {
  for (FrameID frame_id{0}; frame_id < _frames.size(); frame_id++) {
    _free_frames.push_front(frame_id);
  }
  _num_free_frames = _frames.size();
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
  const auto page = &_frames[frame_id];
  return std::make_pair(frame_id, page);
};

FrameID VolatileRegion::get_frame_id_from_ptr(const void* ptr) const {
  // TODO
  // DebugAssert(_frames.data() <= ptr, "Pointer is out of range of region");
  // DebugAssert(ptr < (_frames.data() + capacity() * Page32KiB::size()), "Pointer is out of range of region");
  if (ptr < _frames.begin().operator->() || ptr >= _frames.end().operator->()) {
    return INVALID_FRAME_ID;
  }
  const auto offset = reinterpret_cast<const std::byte*>(ptr) - reinterpret_cast<const std::byte*>(_frames.data());
  return FrameID{offset / Page32KiB::size()};
}

Page32KiB* VolatileRegion::get_page(const FrameID frame_id) {
  DebugAssert(frame_id < capacity(), "Cannot request a frame id larger than capacity.");
  return &_frames[frame_id];
}

void VolatileRegion::deallocate(FrameID frame_id) {
  std::lock_guard<std::mutex> lock_guard(_mutex);

  DebugAssert(frame_id < capacity(), "Cannot request a frame id larger than capacity.");
  _free_frames.push_front(frame_id);
  _num_free_frames++;
};

size_t VolatileRegion::capacity() const {
  return _frames.size();
}

size_t VolatileRegion::size() const {
  return capacity() - _num_free_frames;
}

}  // namespace hyrise
