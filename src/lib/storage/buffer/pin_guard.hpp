#pragma once

#include "storage/buffer/buffer_managed_ptr.hpp"
#include "types.hpp"

namespace hyrise {
/**
 * A Pin Guard ensure that that a pointer/page is unpinned when it goes out of scope. This garantuees exceptions safetiness. It works like std::lock_guard.
*/
template <class T>
class PinGuard : public Noncopyable {
 public:
  explicit PinGuard(BufferManagedPtr<T> ptr, const bool dirty) : _dirty(dirty), _ptr(ptr) {
    _ptr.pin();
  }

  ~PinGuard() {
    _ptr.unpin(_dirty);  // TODO: This could actually happen without page table lookup.
  }

  static PinGuard create(const pmr_vector<T>& vector, const bool dirty) {
    return PinGuard(vector.begin().get_ptr(), dirty);  // TODO: Benchmark this access pattern
  }

  static PinGuard create(const std::shared_ptr<T>& ptr, const bool dirty) {
    return PinGuard(ptr.get(), dirty);  // TODO: Benchmark this access pattern,
  }

 private:
  BufferManagedPtr<T> _ptr;  // TODO: Could this be a reference?
  const bool _dirty;
};

// TODO: Add pinguard with reference to vector instead of shared ptr. Or from other pointer.

}  // namespace hyrise