#include "volatile_region.hpp"
#include "utils/assert.hpp"

namespace hyrise {

VolatileRegion::VolatileRegion(const size_t num_bytes) : _num_bytes(num_bytes), _frames(num_bytes / sizeof(Page32KiB)) {
  for (FrameID frame_id{0}; frame_id < _frames.size() - 1; frame_id++) {
    const auto frame = reinterpret_cast<Page32KiB**>(_frames.data() + frame_id);
    *frame = _frames.data() + (frame_id + 1);
  }
  _free_list = &_frames[0];
  _num_free_frames = _frames.size();
  Assert(_num_free_frames > 0, "There should be at least one free frame in the volatile region while setting up.");
}

std::pair<FrameID, Page32KiB*> VolatileRegion::allocate() {
  // std::lock_guard<std::mutex> lock_guard(_mutex);

  if (_num_free_frames <= 0) {
    return std::make_pair(INVALID_FRAME_ID, nullptr);
  }
  auto free_frame = _free_list;
  const auto new_free_frame = reinterpret_cast<Page32KiB**>(free_frame);
  _free_list = *new_free_frame;

  _num_free_frames--;
  const auto frame_id = get_frame_id_from_ptr(free_frame);
  return std::make_pair(frame_id, free_frame);
};

FrameID VolatileRegion::get_frame_id_from_ptr(const void* ptr) const {
  // TODO: This could be made branchless actually
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
  // std::lock_guard<std::mutex> lock_guard(_mutex);
  DebugAssert(frame_id < capacity(), "Cannot request a frame id larger than capacity.");

  auto old_free_list = _free_list;
  _free_list = _frames.data() + frame_id;
  auto free_list_cast = reinterpret_cast<Page32KiB**>(_free_list);
  *free_list_cast = old_free_list;
  _num_free_frames++;
};

size_t VolatileRegion::capacity() const {
  return _frames.size();
}

size_t VolatileRegion::size() const {
  return capacity() - _num_free_frames;
}

}  // namespace hyrise
