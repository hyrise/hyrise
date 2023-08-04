#pragma once

#include <boost/container/pmr/memory_resource.hpp>
#include <boost/move/utility.hpp>
#include "storage/buffer/buffer_pool_allocator_observer.hpp"

#include "utils/assert.hpp"

namespace hyrise {

/**^
 * The BufferPoolAllocator is a custom, polymorphic allocator that uses the BufferManager to allocate and deallocate pages.
 * 
 * TODO: Combine this allocator with scoped allocator to use same page like monotonic buffer resource for strings
*/
template <class T>
class BufferPoolAllocator {
 public:
  using value_type = T;

  BufferPoolAllocator() : _memory_resource(boost::container::pmr::new_delete_resource()) {
    DebugAssert(_memory_resource != nullptr, "_memory_resource is empty");
  }

  BufferPoolAllocator(boost::container::pmr::memory_resource* memory_resource,
                      std::shared_ptr<BufferPoolAllocatorObserver> observer = nullptr)
      : _memory_resource(memory_resource), _observer(observer) {
    DebugAssert(_memory_resource != nullptr, "_memory_resource is empty");
  }

  BufferPoolAllocator(const BufferPoolAllocator& other) noexcept {
    _memory_resource = other.memory_resource();
    _observer = other.current_observer();
    DebugAssert(_memory_resource != nullptr, "_memory_resource is empty");
  }

  template <class U>
  BufferPoolAllocator(const BufferPoolAllocator<U>& other) noexcept {
    _memory_resource = other.memory_resource();
    _observer = other.current_observer();
    DebugAssert(_memory_resource != nullptr, "_memory_resource is empty");
  }

  BufferPoolAllocator& operator=(const BufferPoolAllocator& other) noexcept {
    _memory_resource = other.memory_resource();
    _observer = other.current_observer();
    DebugAssert(_memory_resource != nullptr, "_memory_resource is empty");
    return *this;
  }

  template <class U>
  bool operator==(const BufferPoolAllocator<U>& other) const noexcept {
    return _memory_resource == other.memory_resource() && _observer.lock() == other.current_observer().lock();
  }

  template <class U>
  bool operator!=(const BufferPoolAllocator<U>& other) const noexcept {
    return _memory_resource != other.memory_resource() || _observer.lock() != other.current_observer().lock();
  }

  [[nodiscard]] T* allocate(std::size_t n) {
    auto ptr = _memory_resource->allocate(sizeof(value_type) * n, alignof(T));
    if (auto observer = _observer.lock()) {
      observer->on_allocate(ptr);
    }
    return static_cast<T*>(ptr);
  }

  void deallocate(T* ptr, std::size_t n) {
    // TODO: Count deallocates for nested resources
    if (auto observer = _observer.lock()) {
      observer->on_deallocate(ptr);
    }
    _memory_resource->deallocate(ptr, sizeof(value_type) * n, alignof(T));
  }

  boost::container::pmr::memory_resource* memory_resource() const noexcept {
    return _memory_resource;
  }

  BufferPoolAllocator select_on_container_copy_construction() const noexcept {
    DebugAssert(_memory_resource != nullptr, "_memory_resource is empty");

    return BufferPoolAllocator(_memory_resource, _observer.lock());
  }

  void register_observer(std::shared_ptr<BufferPoolAllocatorObserver> observer) {
    if (!_observer.expired()) {
      Fail("An observer is already registered");
    }
    _observer = observer;
  }

  std::weak_ptr<BufferPoolAllocatorObserver> current_observer() const {
    return _observer;
  }

 private:
  boost::container::pmr::memory_resource* _memory_resource;
  std::weak_ptr<BufferPoolAllocatorObserver> _observer;
};

}  // namespace hyrise