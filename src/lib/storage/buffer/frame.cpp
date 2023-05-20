#include "storage/buffer/types.hpp"

namespace hyrise {

bool Frame::can_evict() const {
  return is_resident() && !is_pinned();
}

void Frame::try_set_dirty(const bool new_dirty) {
  bool expected = dirty.load();
  bool desired = expected | new_dirty;
  while (!dirty.compare_exchange_strong(expected, desired)) {
    expected = dirty.load();
    desired = expected | new_dirty;
  }
}

bool Frame::is_resident() const {
  DebugAssert(state.load() != State::Resident || data != nullptr,
              "Frame::is_resident() called on frame with data == nullptr");
  return state.load() == State::Resident;
}

bool Frame::is_invalid() const {
  return page_type == PageType::Invalid;
}

bool Frame::is_pinned() const {
  return pin_count.load() > 0;
}

void Frame::set_evicted() {
  state.store(State::Evicted);
}

void Frame::set_resident() {
  state.store(State::Resident);
}

void Frame::set_referenced() {
  referenced.store(true);
}

bool Frame::try_second_chance_evictable() {
  auto was_referenced = referenced.exchange(false);
  return !was_referenced;
}

void Frame::copy_data_to(const FramePtr& target_frame) const {
  const auto num_bytes = bytes_for_size_type(size_type);
  DebugAssert(target_frame->size_type == size_type, "Frame::copy_data_to() called with different size types");
  std::memcpy(target_frame->data, data, num_bytes);
}

template <PageType clone_page_type>
FramePtr Frame::clone_and_attach_sibling() {
  DebugAssert(page_type != clone_page_type, "Frame::clone_and_attach_sibling() called with same page type");
  DebugAssert(sibling_frame == nullptr,
              "Frame::clone_and_attach_sibling() called on frame with sibling_frame != nullptr");
  auto new_frame = make_frame(page_id, size_type, clone_page_type);
  sibling_frame = new_frame.get();
  new_frame->sibling_frame = this;
  return new_frame;
}

template FramePtr Frame::clone_and_attach_sibling<PageType::Dram>();
template FramePtr Frame::clone_and_attach_sibling<PageType::Numa>();

void Frame::clear() {
  state.store(State::Evicted);
  dirty.store(false);
  pin_count.store(0);
  data = nullptr;
  referenced = true;
  eviction_timestamp.store(0);
}

void Frame::pin() {
  pin_count.fetch_add(1, std::memory_order_relaxed);
}

bool Frame::unpin() {
  auto old_pin_count = pin_count.fetch_sub(1, std::memory_order_relaxed);
  DebugAssert(old_pin_count > 0, "Frame::unpin() called on frame with pin_count == 0");
  return old_pin_count == 1;
}

void Frame::wait_until_unpinned() const {
  constexpr auto max_retries = 10000;
  auto retries = size_t{0};
  while (is_pinned()) {
    ++retries;
    std::this_thread::yield();
    if (retries > max_retries) {
      Fail("Frame::wait_until_unpinned() timed out");
    }
  }
}

bool Frame::is_referenced() const {
  return referenced.load();
}

void Frame::increase_ref_count() {
  _ref_count.fetch_add(1, std::memory_order_relaxed);
}

void Frame::decrease_ref_count() {
  _ref_count.fetch_sub(1, std::memory_order_relaxed);
}

bool Frame::is_dirty() const {
  return dirty.load();
}

Frame::~Frame() {
  //TODO DebugAssert(!is_resident(), "Frame was deleted while still resident");
}

// Friend function used by the FramePtr intrusive_ptr to increase the ref_count. This functions should not be called directly.
inline void intrusive_ptr_add_ref(Frame* frame) {
  DebugAssert(frame != nullptr, "Frame is nullptr");
  frame->_ref_count.fetch_add(1, std::memory_order_release);
}

// Friend function used by the FramePtr intrusive_ptr to decrease the ref_count. This functions also avoids circular dependencies between the sibling frame.This functions should not be called directly.
inline void intrusive_ptr_release(Frame* frame) {
  DebugAssert(frame != nullptr, "Frame is nullptr");

  // TODO: Handle reference count with sibling frame
  if (frame->_ref_count.fetch_sub(1, std::memory_order_release) == 1) {
    // Source: https://www.boost.org/doc/libs/1_61_0/doc/html/atomic/usage_examples.html
    std::atomic_thread_fence(std::memory_order_acquire);
    delete frame;
  }
}

std::size_t Frame::_internal_ref_count() const {
  return _ref_count.load();
}

}  // namespace hyrise