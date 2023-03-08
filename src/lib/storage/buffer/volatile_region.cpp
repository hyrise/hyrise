#include "volatile_region.hpp"
#include <boost/align/aligned_alloc.hpp>
#include "utils/assert.hpp"

namespace hyrise {

VolatileRegion::VolatileRegion(const size_t num_bytes, const PageSizeType size_type)
    : _num_frames((num_bytes / bytes_for_size_type(size_type))), _size_type(size_type) {
  _frames = allocate_aligned_frames(_num_frames, size_type);

  for (FrameID frame_id{0}; frame_id < _num_frames - 1; frame_id++) {
    auto frame = reinterpret_cast<std::byte**>(_frames + frame_id * bytes_for_size_type(size_type));
    *frame = _frames + (frame_id + 1) * bytes_for_size_type(size_type);
  }
  _free_list = _frames;
  _num_free_frames = _num_frames;
  Assert(_num_free_frames > 0, "There should be at least one free frame in the volatile region while setting up.");
}

VolatileRegion::~VolatileRegion() {
  boost::alignment::aligned_free(_frames);
}

std::byte* VolatileRegion::allocate_aligned_frames(const size_t num_frames, const PageSizeType size_type) {
  return static_cast<std::byte*>(
      boost::alignment::aligned_alloc(PAGE_ALIGNMENT, num_frames * bytes_for_size_type(size_type)));
}

std::pair<FrameID, std::byte*> VolatileRegion::allocate() {
  if (_num_free_frames <= 0) {
    return std::make_pair(INVALID_FRAME_ID, nullptr);
  }
  auto free_frame = _free_list;
  const auto new_free_frame = reinterpret_cast<std::byte**>(free_frame);
  _free_list = *new_free_frame;

  _num_free_frames--;
  const auto frame_id = to_frame_id(free_frame);
  // memset(free_frame, 0, bytes_for_size_type(_size_type));
  return std::make_pair(frame_id, free_frame);
};

FrameID VolatileRegion::to_frame_id(const void* ptr) const {
  // TODO: This could be made branchless actually
  if (ptr < _frames || ptr >= _frames + _num_frames * bytes_for_size_type(_size_type)) {
    return INVALID_FRAME_ID;
  }
  const auto offset = reinterpret_cast<const std::byte*>(ptr) - reinterpret_cast<const std::byte*>(_frames);
  return FrameID{offset / bytes_for_size_type(_size_type)};
}

std::byte* VolatileRegion::get_page(const FrameID frame_id) {
  DebugAssert(frame_id < capacity(), "Cannot request a frame id larger than capacity.");
  return _frames + frame_id * bytes_for_size_type(_size_type);
}

void VolatileRegion::deallocate(FrameID frame_id) {
  DebugAssert(frame_id < capacity(), "Cannot request a frame id larger than capacity.");

  auto old_free_list = _free_list;
  _free_list = _frames + frame_id * bytes_for_size_type(_size_type);
  auto free_list_cast = reinterpret_cast<std::byte**>(_free_list);
  *free_list_cast = old_free_list;
  _num_free_frames++;
};

size_t VolatileRegion::capacity() const {
  return _num_frames;
}

size_t VolatileRegion::size() const {
  return capacity() - _num_free_frames;
}

PageSizeType VolatileRegion::get_size_type() const {
  return _size_type;
}

}  // namespace hyrise
