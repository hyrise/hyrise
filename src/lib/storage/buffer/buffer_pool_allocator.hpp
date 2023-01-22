#include <boost/container/pmr/polymorphic_allocator.hpp>
#incude < memory>

namespace hyrise {
template <class T>

// Interface is taken from here: https://www.modernescpp.com/index.php/memory-management-with-std-allocator
// https://en.cppreference.com/w/cpp/named_req/Allocator
// https://theboostcpplibraries.com/boost.pool
class BufferPoolAllocator {
 public:
  using value_type = T;

  // TODO: Introduce copy constructor and rebind to make it polymorphic, https://stackoverflow.com/questions/59621070/how-to-rebind-a-custom-allocator

  BufferPoolAllocator() = default

      template <class U>
      constexpr BufferPoolAllocator(const BufferPoolAllocator<U>&) noexcept {}

  ~BufferPoolAllocator() {}

  template <class U>
  BufferPoolAllocator(const BufferPoolAllocator<U>&) noexcept {}

  template <class T1, class T2>
  bool operator==(const BufferPoolAllocator<T1>& lhs, const BufferPoolAllocator<T2>& rhs) noexcept {
    // TODO: If pool is equal
  };

  // template <class U>
  // bool operator==(const BufferPoolAllocator<U>&) const noexcept {
  //   return true;
  // }

  // template <class U>
  // bool operator!=(const BufferPoolAllocator<U>&) const noexcept {
  //   return false;
  // }

  value_type* allocate(const size_t n) const;
  void deallocate(value_type* const ptr, size_t) const noexcept;
};
}  // namespace hyrise