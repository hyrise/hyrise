#pragma once

#include <boost/interprocess/offset_ptr.hpp>
#include "buffer_pool_resource.hpp"

namespace hyrise {
template <class T>

// Interface is taken from here: https://www.modernescpp.com/index.php/memory-management-with-std-allocator
// https://en.cppreference.com/w/cpp/named_req/Allocator
// https://theboostcpplibraries.com/boost.pool
class BufferPoolAllocator {
 public:
  using value_type = T;
  using pointer = BufferManagedPtr<T>;
  using const_pointer = const BufferManagedPtr<T>;
  using void_pointer = BufferManagedPtr<void>;
  using difference_type = typename pointer::difference_type;

  // TODO: Introduce copy constructor and rebind to make it polymorphic, https://stackoverflow.com/questions/59621070/how-to-rebind-a-custom-allocator
  // TODO: Get default resource should use singleton

  BufferPoolAllocator() = default;

  explicit BufferPoolAllocator(BufferPoolResource* resource) : _resource(resource) {}

  template <class U>
  BufferPoolAllocator(const BufferPoolAllocator<U>& other) noexcept {
    _resource = other.resource();
  }

  template <class U>
  struct rebind {
    typedef BufferPoolAllocator<U> other;
  };

  template <class U>
  bool operator==(const BufferPoolAllocator<U>& other) const noexcept {
    return _resource == other.resource(); // TODO
  }

  template <class U>
  bool operator!=(const BufferPoolAllocator<U>& other) const noexcept {
    return _resource != other.resource(); // TODO
  }

  [[nodiscard]] constexpr pointer allocate(std::size_t n) {
    // If n == 1, its a normal object, if n > 1, 
    // its an array like for a vecor
    // TODO: How about alignment or done by memory resource?
    return static_cast<pointer>(_resource->allocate(sizeof(value_type) * n));
  }

  constexpr void deallocate(pointer const ptr, std::size_t n) const noexcept {
    _resource->deallocate(static_cast<void_pointer>(ptr), sizeof(value_type) * n);
  }

  BufferPoolResource* resource() const noexcept {
    return _resource;
  }

  // Construct, destroy etc
  // template <class U, class... Args>
  // void construct(U* p, Args&&... args) {
  //   // TODO: construct etc, https://stackoverflow.com/questions/28521203/custom-pointer-types-and-container-allocator-typedefs
  //   ::new (static_cast<void*>(p)) U(std::forward<Args>(args)...);
  // }

 private:
  BufferPoolResource* _resource;
};
}  // namespace hyrise