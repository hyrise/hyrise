#pragma once

#include <boost/container/small_vector.hpp>
#include <type_traits>
#include "hyrise.hpp"
#include "storage/buffer/buffer_managed_ptr.hpp"
#include "storage/buffer/buffer_pool_allocator.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * A Pin Guard ensure that that a pointer/page is unpinned when it goes out of scope. This garantuees exceptions safetiness using the RAII pattern.
*/
template <class PinnableType>
class PinGuard : public Noncopyable {
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

/**
 * The AllocatorPinGuard observes all allocations and deallocations of a given allocator and pins the allocated pages during the life-time of the
 * AllocatorPinGuard including resizes of a data strcutures. All pages are unpinned when the AllocatorPinGuard is destroyed. The AllocatorPinGuard is not thread-safe.
*/
template <typename Allocator>
class AllocatorPinGuard : public Noncopyable {
  struct Observer : Noncopyable, BufferPoolAllocatorObserver {
    void on_allocate(const PageID page_id, const PageSizeType size_type) override {
      Hyrise::get().buffer_manager.pin_page(page_id, size_type);
      _pinned_pages.push_back(page_id);
    }

    void on_deallocate(const PageID page_id) override {
      Hyrise::get().buffer_manager.unpin_page(page_id, false);
      _pinned_pages.erase(std::remove(_pinned_pages.begin(), _pinned_pages.end(), page_id), _pinned_pages.end());
    }

    ~Observer() {
      for (const auto& page_id : _pinned_pages) {
        Hyrise::get().buffer_manager.unpin_page(page_id, false);
      }
    }

    boost::container::small_vector<PageID, 5> _pinned_pages;
  };

 public:
  AllocatorPinGuard(Allocator& allocator) : _observer(std::make_shared<Observer>()) {
    if constexpr (std::is_same_v<Allocator, BufferPoolAllocator<typename Allocator::value_type>>) {
      allocator.register_observer(_observer);
    } else if constexpr (std::is_same_v<Allocator, PolymorphicAllocator<typename Allocator::value_type>>) {
      allocator.outer_allocator().register_observer(_observer);
    } else {
      Fail("AllocatorPinGuard is not implemented for given allocator");
    }
  }

 private:
  std::shared_ptr<Observer> _observer;
};

}  // namespace hyrise