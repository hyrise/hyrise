#include "pin_guard.hpp"

namespace hyrise {

void AllocatorPinGuard::Observer::on_allocate(const void* ptr) {
  const auto page_id = BufferManager::get().find_page(ptr);
  PinnedPageIds::add_pin(page_id);
}

void AllocatorPinGuard::Observer::on_deallocate(const void* ptr) {}

AllocatorPinGuard::Observer::~Observer() {
  PinnedPageIds::unpin_all();
}
}  // namespace hyrise