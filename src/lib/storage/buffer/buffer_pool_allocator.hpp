#pragma once

#include <boost/container/pmr/memory_resource.hpp>
#include <boost/container/small_vector.hpp>
#include <boost/move/utility.hpp>
#include "storage/buffer/buffer_managed_ptr.hpp"
#include "storage/buffer/buffer_manager.hpp"
#include "utils/assert.hpp"

namespace hyrise {

class BufferPoolAllocatorObserver {
 public:
  virtual void on_allocate(const PageID page_id, const PageSizeType size_type) = 0;
  virtual void on_deallocate(const PageID page_id) = 0;
};

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
    if (auto observer = _observer.lock()) {
      const auto page_id = ptr.get_page_id();
      const auto size_type = ptr.get_size_type();
      observer->on_allocate(page_id, size_type);
    }
    return ptr;
  }

  void deallocate(pointer const ptr, std::size_t n) {
    if (auto observer = _observer.lock()) {
      const auto page_id = ptr.get_page_id();
      observer->on_deallocate(page_id);
    }
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

  void register_observer(std::shared_ptr<BufferPoolAllocatorObserver> observer) {
    _observer = observer;
  }

 private:
  std::weak_ptr<BufferPoolAllocatorObserver> _observer;
  // std::mutex _page_ids_mutex;
  BufferManager* _buffer_manager;
};
}  // namespace hyrise