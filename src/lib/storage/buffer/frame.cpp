#include "storage/buffer/types.hpp"

namespace hyrise {

void Frame::init(const PageID page_id) {
  this->page_id = page_id;
  pin_count.store(0);
  eviction_timestamp.store(0);
  state.store(State::Resident);
}

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
  return state.load() == State::Resident;
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

void Frame::clear() {
  page_id = INVALID_PAGE_ID;
  state.store(State::Evicted);
  dirty.store(false);
  pin_count.store(0);
  data = nullptr;
  eviction_timestamp.store(0);
}
}  // namespace hyrise