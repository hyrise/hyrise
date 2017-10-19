#pragma once

#include <experimental/memory_resource>
#include <memory>

namespace opossum {

// We need a wrapper around std::experimental::pmr::polymorphic_allocator because tbb::concurrent_vector still relies on
// rebind<U>::other (which is deprecated in C++17).
// See src/lib/utils/boost_default_memory_resource.cpp for an implementation of a default memory resource.
template <class T>
class PolymorphicAllocator : public std::experimental::pmr::polymorphic_allocator<T> {
 public:
  template <class U>
  struct rebind {
    typedef PolymorphicAllocator<U> other;
  };

  PolymorphicAllocator() {}
  template <class U>
  PolymorphicAllocator(const std::experimental::pmr::polymorphic_allocator<U>& alloc)  // NOLINT(runtime/explicit)
      : std::experimental::pmr::polymorphic_allocator<T>(alloc) {}
  template <class U>
  PolymorphicAllocator(const PolymorphicAllocator<U>& other)
      : std::experimental::pmr::polymorphic_allocator<T>(other) {}
  PolymorphicAllocator(std::experimental::pmr::memory_resource* m_resource)  // NOLINT(runtime/explicit)
      : std::experimental::pmr::polymorphic_allocator<T>(m_resource) {}
};

}  // namespace opossum
