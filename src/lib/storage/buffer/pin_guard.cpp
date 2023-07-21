#include "pin_guard.hpp"

namespace hyrise {

void AllocatorPinGuard::Observer::on_allocate(const PageID page_id) {
  PinnedPageIds::add_pin(page_id);
}

void AllocatorPinGuard::Observer::on_deallocate(const PageID page_id) {}

AllocatorPinGuard::Observer::~Observer() {
  PinnedPageIds::unpin_all();
}
}  // namespace hyrise