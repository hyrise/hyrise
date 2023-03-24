#pragma once

#include <type_traits>
#include "storage/buffer/buffer_managed_ptr.hpp"
#include "storage/buffer/buffer_pool_allocator.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * A Pin Guard ensure that that a pointer/page is unpinned when it goes out of scope. This garantuees exceptions safetiness usinf the RAII pattern.
*/

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

  void track_page(const PageID page_id){

  };

  ~PinGuard() {
    _ptr.unpin(_dirty);  // TODO: This could actually happen without page table lookup.
  }

 private:
  PinnableType& _object;
  BufferManagedPtr<typename PinnableType::value_type> _ptr;
  bool _dirty;
};

template <typename T>
class AllocatorPinGuard {
 public:
  AllocatorPinGuard(BufferPoolAllocator<T>& allocator) : _pins(std::make_shared<std::vector<PageID>>()) {
    // _buffer_manager = allocator.register_pin_guard(_pins);
  }

  // AllocatorPinGuard(BufferPoolAllocator<T> allocator) : _pins(std::make_shared<BufferManagerPins>()) {
  //   _buffer_manager = allocator->register_pin_guard(_pins);
  // }

  ~AllocatorPinGuard() {
    // for (const auto page_id : *_pins) {
    //   _buffer_manager->unpin_page(page_id);
    // }
  }

 private:
  BufferManager* _buffer_manager;
  std::shared_ptr<std::vector<PageID>> _pins;
};

}  // namespace hyrise