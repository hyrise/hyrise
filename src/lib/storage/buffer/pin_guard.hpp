#pragma once

#include <boost/container/small_vector.hpp>
#include <type_traits>
#include "hyrise.hpp"
#include "storage/buffer/buffer_managed_ptr.hpp"
#include "storage/buffer/buffer_pool_allocator.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * A Pin Guard ensure that that a pointer/page is unpinned when it goes out of scope. This garantuees exceptions safetiness usinf the RAII pattern.
*/

// TODO: Signify read/write intent of PinGuard, AllocatorPinGuard is always "Write"

template <class PinnableType>
class PinGuard {  // TODO: Non copy
 public:
  explicit PinGuard(PinnableType& object, const bool dirty)
      : _object(object), _dirty(dirty), _ptr(_object.begin().get_ptr()) {
    _ptr.pin();
  }

  void repin(const bool dirty) {
    _ptr.unpin(_dirty);
    _dirty = dirty;
    _ptr = _object.begin().get_ptr();
  }

  ~PinGuard() {
    _ptr.unpin(_dirty);  // TODO: This could actually happen without page table lookup if we save the raw buffer frame
  }

 private:
  PinnableType& _object;
  BufferManagedPtr<typename PinnableType::value_type> _ptr;
  bool _dirty;
};

class AllocatorPinGuard : public BufferPoolAllocatorObserver {
 public:
  ~AllocatorPinGuard() {
    for (const auto& page_id : _pinned_pages) {
      Hyrise::get().buffer_manager.unpin_page(page_id, true);
    }
  }

  void on_allocate(const PageID page_id, const PageSizeType size_type) override {
    Hyrise::get().buffer_manager.pin_page(page_id, size_type);
    _pinned_pages.push_back(page_id);
  }

  void on_deallocate(const PageID page_id) override {
    Hyrise::get().buffer_manager.unpin_page(page_id, false);
    _pinned_pages.erase(std::remove(_pinned_pages.begin(), _pinned_pages.end(), page_id), _pinned_pages.end());
  }

 private:
  boost::container::small_vector<PageID, 5> _pinned_pages;
};

template <typename Allocator>
std::shared_ptr<AllocatorPinGuard> make_allocator_pin_guard(Allocator& allocator) {
  auto guard = std::make_shared<AllocatorPinGuard>();
  if constexpr (std::is_same_v<Allocator, BufferPoolAllocator<typename Allocator::value_type>>) {
    allocator.register_observer(guard);
  } else if constexpr (std::is_same_v<Allocator, PolymorphicAllocator<typename Allocator::value_type>>) {
    allocator.outer_allocator().register_observer(guard);
  } else {
    Fail("AllocatorPinGuard is not implemented for allocator");
  }

  return guard;
}
}  // namespace hyrise