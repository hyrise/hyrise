#pragma once

#include <boost/container/pmr/memory_resource.hpp>
#include <boost/move/utility.hpp>
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
  using const_pointer = BufferManagedPtr<const T>;
  using void_pointer = BufferManagedPtr<void>;
  using difference_type = typename pointer::difference_type;

  // TODO: Introduce copy constructor and rebind to make it polymorphic, https://stackoverflow.com/questions/59621070/how-to-rebind-a-custom-allocator
  // TODO: Get default resource should use singleton

  BufferPoolAllocator() = default;

  explicit BufferPoolAllocator(BufferPoolResource* resource) : _resource(resource) {}

  // FIXME: This does not work
  BufferPoolAllocator(boost::container::pmr::memory_resource* resource) : _resource(nullptr) {}

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
    return _resource == other.resource();  // TODO
  }

  template <class U>
  bool operator!=(const BufferPoolAllocator<U>& other) const noexcept {
    return _resource != other.resource();  // TODO
  }

  [[nodiscard]] pointer allocate(std::size_t n) {
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

  BufferPoolAllocator select_on_container_copy_construction() const noexcept {
    return BufferPoolAllocator();
  }

  // TODO: LIke interprocess, use pointer class directly instead of T*
  template <typename U, class Args>
  void construct(const BufferManagedPtr<U>& ptr, BOOST_FWD_REF(Args) args) {
    ::new ((void*)ptr.operator->()) U(boost::forward<Args>(args));
  }

  // TODO: Add construct_at

  template <class U>
  void destroy(U* ptr) {
    ptr->~U();
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