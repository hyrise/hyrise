#pragma once

#include <boost/container/pmr/memory_resource.hpp>
#include <boost/move/utility.hpp>
#include "storage/buffer/buffer_managed_ptr.hpp"
#include "storage/buffer/buffer_manager.hpp"
#include "utils/assert.hpp"

namespace hyrise {

// Interface is taken from here: https://www.modernescpp.com/index.php/memory-management-with-std-allocator
// https://en.cppreference.com/w/cpp/named_req/Allocator
// https://theboostcpplibraries.com/boost.pool
template <class T>
class BufferPoolAllocator {
 public:
  using value_type = T;
  using pointer = BufferManagedPtr<T>;
  using const_pointer = BufferManagedPtr<const T>;
  using void_pointer = BufferManagedPtr<void>;
  using difference_type = typename pointer::difference_type;

  // TODO: Introduce copy constructor and rebind to make it polymorphic, https://stackoverflow.com/questions/59621070/how-to-rebind-a-custom-allocator
  BufferPoolAllocator() : _buffer_manager(&BufferManager::get_global_buffer_manager()) {}

  BufferPoolAllocator(BufferManager* buffer_manager) : _buffer_manager(buffer_manager) {}

  BufferPoolAllocator(boost::container::pmr::memory_resource* resource) : _buffer_manager(nullptr) {
    Fail("The current BufferPoolAllocator cannot take a boost memory_resource");
  }

  template <class U>
  BufferPoolAllocator(const BufferPoolAllocator<U>& other) noexcept {
    _buffer_manager = other.buffer_manager();
  }

  template <class U>
  struct rebind {
    typedef BufferPoolAllocator<U> other;
  };

  template <class U>
  bool operator==(const BufferPoolAllocator<U>& other) const noexcept {
    return _buffer_manager == other.buffer_manager();
  }

  template <class U>
  bool operator!=(const BufferPoolAllocator<U>& other) const noexcept {
    return _buffer_manager != other.buffer_manager();
  }

  [[nodiscard]] pointer allocate(std::size_t n) {
    auto ptr = static_cast<pointer>(_buffer_manager->allocate(sizeof(value_type) * n, alignof(T)));
    return ptr;
  }

  constexpr void deallocate(pointer const ptr, std::size_t n) const noexcept {
    _buffer_manager->deallocate(static_cast<void_pointer>(ptr), sizeof(value_type) * n, alignof(T));
  }

  BufferManager* buffer_manager() const noexcept {
    return _buffer_manager;
  }

  BufferPoolAllocator select_on_container_copy_construction() const noexcept {
    return BufferPoolAllocator(_buffer_manager);
  }

  template <typename U, class... Args>
  void construct(const U* ptr, BOOST_FWD_REF(Args)... args) {
    ::new ((void*)ptr) U(boost::forward<Args>(args)...);
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
  BufferManager* _buffer_manager;
};
}  // namespace hyrise