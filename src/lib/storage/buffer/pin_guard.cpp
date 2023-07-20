#include "pin_guard.hpp"

namespace hyrise {

void AllocatorPinGuard::Observer::on_allocate(const PageID page_id) {
  PinnedPageIds::add_pin(page_id);
}

void AllocatorPinGuard::Observer::on_deallocate(const PageID page_id) {}

void AllocatorPinGuard::Observer::on_pin(const PageID page_id) {
  BufferManager::get().pin_shared(page_id, AccessIntent::Write);
}

void AllocatorPinGuard::Observer::on_unpin(const PageID page_id) {
  BufferManager::get().set_dirty(page_id);
  BufferManager::get().unpin_shared(page_id);
}

AllocatorPinGuard::Observer::~Observer() {
  PinnedPageIds::unpin_all();
}
}  // namespace hyrise