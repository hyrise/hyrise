#pragma once

#include <boost/container/pmr/memory_resource.hpp>
#include <boost/move/utility.hpp>
#include "buffer_pool_resource.hpp"
#include "utils/assert.hpp"

namespace hyrise {
template <class T>

// Interface is taken from here: https://www.modernescpp.com/index.php/memory-management-with-std-allocator
// https://en.cppreference.com/w/cpp/named_req/Allocator
// https://theboostcpplibraries.com/boost.pool
class BufferPoolAllocator {
 public:
  using value_type = T;
  using pointer = BufferManagedPtr<T>;
  using const_pointer = BufferManagedPtr<const T>;
  using void_pointer = BufferManagedPtr<void>;
  using difference_type = typename pointer::difference_type;

  // TODO: Introduce copy constructor and rebind to make it polymorphic, https://stackoverflow.com/questions/59621070/how-to-rebind-a-custom-allocator
  // TODO: This should use the global singleton 
  BufferPoolAllocator() = default;

  explicit BufferPoolAllocator(BufferPoolResource* resource) : _resource(resource) {}

  BufferPoolAllocator(boost::container::pmr::memory_resource* resource) : _resource(nullptr) {
    Fail("The current BufferPoolAllocator cannot take a boost memory_resource");
  }

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
    return _resource == other.resource();
  }

  template <class U>
  bool operator!=(const BufferPoolAllocator<U>& other) const noexcept {
    return _resource != other.resource();
  }

  [[nodiscard]] pointer allocate(std::size_t n) {
    return static_cast<pointer>(_resource->allocate(sizeof(value_type) * n, alignof(T)));
  }

  constexpr void deallocate(pointer const ptr, std::size_t n) const noexcept {
    _resource->deallocate(static_cast<void_pointer>(ptr), sizeof(value_type) * n,  alignof(T));
  }

  BufferPoolResource* resource() const noexcept {
    return _resource;
  }

  BufferPoolAllocator select_on_container_copy_construction() const noexcept {
    return BufferPoolAllocator();
  }

  template <typename U, class Args>
  void construct(const BufferManagedPtr<U>& ptr, BOOST_FWD_REF(Args) args) {
    ::new (static_cast<void*>(ptr.operator->())) U(boost::forward<Args>(args));
  }

  template <class U>
  void destroy(const BufferManagedPtr<U>& ptr) {
    ptr->~U();
  }

 private:
  BufferPoolResource* _resource;
};
}  // namespace hyrise